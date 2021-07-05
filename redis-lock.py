import threading
import time as mod_time
import uuid
from types import SimpleNamespace
from redis.exceptions import LockError, LockNotOwnedError


class Lock:
    lua_release = None
    lua_extend = None
    lua_reacquire = None

    # KEYS[1] - 锁名称
    # ARGV[1] - 锁 token
    # 如果释放了锁，返回1，否则返回0
    LUA_RELEASE_SCRIPT = """
        local token = redis.call('get', KEYS[1])
        if not token or token ~= ARGV[1] then
            return 0
        end
        redis.call('del', KEYS[1])
        return 1
    """

    # KEYS[1] - 锁名称
    # ARGV[1] - 锁 token
    # ARGV[2] - 锁续期 milliseconds 值
    # ARGV[3] - 如果将额外的时间加到锁现有的TTL上，则为0; 如果替换现有的TTL，则为1
    # 如果锁时间延长则返回1，否则返回0

    # Redis pttl 命令以毫秒为单位返回 key 的剩余过期时间。
    # Redis pexpire 以毫秒为单位设置 key 的生存时间
    LUA_EXTEND_SCRIPT = """
        local token = redis.call('get', KEYS[1])
        if not token or token ~= ARGV[1] then
            return 0
        end
        local expiration = redis.call('pttl', KEYS[1])
        if not expiration then
            expiration = 0
        end
        if expiration < 0 then
            return 0
        end
        local newttl = ARGV[2]
        if ARGV[3] == "0" then
            newttl = ARGV[2] + expiration
        end
        redis.call('pexpire', KEYS[1], newttl)
        return 1
    """

    # KEYS[1] - 锁名称
    # ARGV[1] - 锁 token
    # ARGV[2] - 锁时间 milliseconds 值
    # 如果重新获取了锁时间，则返回1，否则返回0
    LUA_REACQUIRE_SCRIPT = """
        local token = redis.call('get', KEYS[1])
        if not token or token ~= ARGV[1] then
            return 0
        end
        redis.call('pexpire', KEYS[1], ARGV[2])
        return 1
    """

    def __init__(self, redis, name, timeout=None, sleep=0.1, blocking=True, blocking_timeout=None, thread_local=True):
        """
        使用Redis提供的Redis客户端创建一个名为 'name' 的新锁实例。

        timeout - 锁释放时间，可以指定为浮点数或整数，单位是秒
        sleep - 另一个客户端当前持有锁时，每个循环迭代的休眠时间。
        blocking - 另一个客户端当前持有锁时，在调用 'acquire' 上锁时，会阻塞直到之前锁被释放后获取还是立即返回上锁失败
        blocking_timeout - 是 blocking 的超时时间，None 表示永不过期，直到抢到锁为止，也可以指定为浮点数或整数，单位是秒
        thread_local - 锁的 token 是否放置在线程本地存储中。默认情况下，令牌被放置在线程本地存储中
        """
        self.redis = redis
        self.name = name
        self.timeout = timeout
        self.sleep = sleep
        self.blocking = blocking
        self.blocking_timeout = blocking_timeout
        self.thread_local = bool(thread_local)
        self.local = (
            threading.local()
            if self.thread_local
            else SimpleNamespace()  # 一个简单的 object 子类，可以添加和移除属性
        )
        self.local.token = None
        self.register_scripts()  # 实例化的时候就加载 LUA 脚本

    def register_scripts(self):
        """
        加载三个 LUA 脚本
        1. 释放锁的 LUA 脚本
        2. 锁续期的 LUA 脚本
        3. 重新获取锁的 LUA 脚本
        """
        cls = self.__class__
        client = self.redis
        if cls.lua_release is None:
            cls.lua_release = client.register_script(cls.LUA_RELEASE_SCRIPT)
        if cls.lua_extend is None:
            cls.lua_extend = client.register_script(cls.LUA_EXTEND_SCRIPT)
        if cls.lua_reacquire is None:
            cls.lua_reacquire = client.register_script(cls.LUA_REACQUIRE_SCRIPT)

    # 提供 with 语法
    def __enter__(self):
        if self.acquire():
            return self
        raise LockError("Unable to acquire lock within the time specified")

    def __exit__(self, exc_type, exc_value, traceback):
        self.release()

    def acquire(self, blocking=None, blocking_timeout=None, token=None):
        """
        blocking - 是否阻塞获取锁
        blocking_timeout - 获取锁所等待的最大秒数
        token - token必须是一个 bytes 对象或可以用默认编码编码到 bytes 对象的字符串。如果没有指定令牌，将使用 UUID。
        """
        sleep = self.sleep
        if token is None:
            token = uuid.uuid1().hex.encode()
        else:
            encoder = self.redis.connection_pool.get_encoder()
            token = encoder.encode(token)

        # acquire 的 blocking 和 blocking_timeout 参数优先级高于实例化时候的。acquire 的时候没设置就用实例化的时候的
        if blocking is None:
            blocking = self.blocking
        if blocking_timeout is None:
            blocking_timeout = self.blocking_timeout

        # 计算 停止获取锁的时间
        stop_trying_at = None
        if blocking_timeout is not None:
            # time.monotonic()（以小数表示的秒为单位）返回一个单调时钟的值，即不能倒退的时钟。 该时钟不受系统时钟更新的影响。
            stop_trying_at = mod_time.monotonic() + blocking_timeout

        while True:
            # 成功上锁
            if self.do_acquire(token):
                self.local.token = token
                return True
            # 如果锁获取失败，且没有设置 blocking 阻塞，那么立即返回 False
            if not blocking:
                return False
            # 下一次重试获取锁的时间
            next_try_at = mod_time.monotonic() + sleep
            # 如果超过了设置的获取锁的时间还没有获取到锁，那么就直接返回 False
            if stop_trying_at is not None and next_try_at > stop_trying_at:
                return False
            mod_time.sleep(sleep)

    def do_acquire(self, token):
        # 将锁的释放时间转换为毫秒
        if self.timeout:
            # convert to milliseconds
            timeout = int(self.timeout * 1000)
        else:
            timeout = None
        # 上锁。self.name 为锁名称，token 为锁的值。
        # 在 Redis 中如果使用了 NX 选项，SET 命令只有在键值对不存在时，才会进行设置，否则不做赋值操作
        # 在 Redis 中使用 px 来设置 key 的过期时间，单位为毫秒。ex 也可以设置，不过单位为秒
        if self.redis.set(self.name, token, nx=True, px=timeout):
            return True
        return False

    def locked(self):
        """
        判断是否上锁成功
        """
        return self.redis.get(self.name) is not None

    def owned(self):
        """
        如果使用的是 thread_local 存储 token，那么判断这个 token 是否上锁成功。上面的 locked 判断的只是实例的 token 是否上锁成功
        """
        stored_token = self.redis.get(self.name)
        if stored_token and not isinstance(stored_token, bytes):
            encoder = self.redis.connection_pool.get_encoder()
            stored_token = encoder.encode(stored_token)
        return self.local.token is not None and stored_token == self.local.token

    def release(self):
        """
        释放已经获取的锁
        """
        expected_token = self.local.token
        if expected_token is None:
            raise LockError("Cannot release an unlocked lock")
        self.local.token = None
        self.do_release(expected_token)

    def do_release(self, expected_token):
        if not bool(self.lua_release(keys=[self.name], args=[expected_token], client=self.redis)):
            raise LockNotOwnedError("Cannot release a lock that's no longer owned")

    def extend(self, additional_time, replace_ttl=False):
        """
        为已获得的锁增加时间
        additional_time - 可以指定为整数或浮点数，两者都表示要添加的秒数。
        replace_ttl - 如果为False(默认值)，将 'additional_time' 添加到锁现有的 ttl中。如果为 True，将锁的 ttl替换为 'additional_time'。
        """
        if self.local.token is None:
            raise LockError("Cannot extend an unlocked lock")
        # 不能对没有设置超时时间的锁设置新的时间
        if self.timeout is None:
            raise LockError("Cannot extend a lock with no timeout")
        return self.do_extend(additional_time, replace_ttl)

    def do_extend(self, additional_time, replace_ttl):
        # 时间转换为毫秒
        additional_time = int(additional_time * 1000)
        if not bool(
            self.lua_extend(
                keys=[self.name],
                args=[
                    self.local.token,
                    additional_time,
                    replace_ttl and "1" or "0"
                ],
                client=self.redis,
            )
        ):
            raise LockNotOwnedError("Cannot extend a lock that's" " no longer owned")
        return True

    def reacquire(self):
        """
        重新获取之前的锁。超时时间也是之前的
        """
        if self.local.token is None:
            raise LockError("Cannot reacquire an unlocked lock")
        if self.timeout is None:
            raise LockError("Cannot reacquire a lock with no timeout")
        return self.do_reacquire()

    def do_reacquire(self):
        timeout = int(self.timeout * 1000)
        if not bool(self.lua_reacquire(keys=[self.name], args=[self.local.token, timeout], client=self.redis)):
            raise LockNotOwnedError("Cannot reacquire a lock that's no longer owned")
        return True
