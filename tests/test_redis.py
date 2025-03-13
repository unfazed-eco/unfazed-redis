import os
import pytest
import asyncio
from unfazed_redis.backends.namespaceclient import NamespaceClient
from redis.exceptions import ResponseError

HOST = os.getenv("REDIS_HOST", "redis")


@pytest.fixture(scope="session")
def event_loop():
    """创建一个会话级别的事件循环"""
    policy = asyncio.get_event_loop_policy()
    loop = policy.new_event_loop()
    yield loop
    loop.close()


@pytest.fixture(scope="function")
async def client(event_loop):
    """创建一个测试用的 Redis 客户端"""
    client = NamespaceClient(
        f"redis://{HOST}:6379",
        options={"decode_responses": True, "PREFIX": "test", "VERSION": "1"},
    )

    # 清理之前可能存在的测试数据
    await client.delete("foo", "hash_key", "list_key", "set_key", "zset_key", "counter")

    yield client

    # 测试后清理数据
    await client.delete("foo", "hash_key", "list_key", "set_key", "zset_key", "counter")


# 第一部分：基础配置测试
async def test_client_basic_initialization(client) -> None:
    """测试客户端基本初始化"""
    # 测试默认初始化
    client = NamespaceClient(
        location=f"redis://{HOST}:6379", options={"PREFIX": "test", "VERSION": "11"}
    )

    # 测试基本键前缀
    key = client.make_key("test_key")
    assert key == f"{client.options.prefix}:{client.options.version}:test_key"


async def test_client_custom_initialization(client) -> None:
    """测试客户端自定义配置初始化"""
    custom_options = {
        "decode_responses": True,
        "PREFIX": "custom",
        "VERSION": "2",
        "retry": True,
        "socket_timeout": 5.0,
        "socket_connect_timeout": 1.0,
        "socket_keepalive": True,
        "health_check_interval": 30,
    }
    client = NamespaceClient(f"redis://{HOST}:6379", options=custom_options)

    # 验证配置
    assert client.options.decode_responses
    assert client.options.prefix == "custom"
    assert client.options.version == "2"
    assert client.options.retry
    assert client.options.socket_timeout == 5.0

    # 验证键前缀
    key = client.make_key("test_key")
    assert key == "custom:2:test_key"


# 第二部分：基本键值操作测试
async def test_key_basic_operations(client) -> None:
    """测试基本的键值操作"""
    # 测试 set 和 get
    await client.set("basic_key", "value1")
    assert await client.get("basic_key") == "value1"
    assert await client.client.get(client.make_key("basic_key")) == "value1"
    # 测试 delete
    await client.delete("basic_key")
    assert await client.exists("basic_key") == 0


async def test_key_edge_cases(client) -> None:
    """测试键值操作的边界情况"""
    # 测试不存在的键
    assert await client.get("nonexistent_key") is None

    # 测试空值
    await client.set("empty_key", "")
    assert await client.get("empty_key") == ""

    # 测试特殊字符的键
    special_keys = ["key:with:colon", "key with space", "key_with_unicode_中文"]
    for key in special_keys:
        await client.set(key, "value")
        assert await client.get(key) == "value"
        await client.delete(key)


async def test_key_expiration_operations(client) -> None:
    """测试键过期相关操作"""
    # 测试设置过期时间
    await client.set("expire_key", "value", timeout=1)
    ttl = await client.ttl("expire_key")
    assert 0 < ttl <= 1

    ttl = await client.client.ttl(client.make_key("expire_key"))
    assert 0 < ttl <= 1

    # 测试永久键
    await client.set("permanent_key", "value")
    assert await client.ttl("permanent_key") == -1
    assert await client.client.ttl(client.make_key("permanent_key")) == -1
    # 测试更新过期时间
    await client.expire("permanent_key", 5)
    assert 0 < await client.ttl("permanent_key") <= 5
    assert 0 < await client.client.ttl(client.make_key("permanent_key")) <= 5


async def test_hash_basic_operations(client) -> None:
    """测试哈希表的基本操作"""
    # 准备测试数据
    await client.hmset("hash_key", {"field1": "value1", "field2": "value2"})

    # 测试基本的 CRUD 操作
    assert await client.hget("hash_key", "field1") == "value1"
    assert await client.hgetall("hash_key") == {"field1": "value1", "field2": "value2"}
    assert await client.hexists("hash_key", "field1")
    assert set(await client.hkeys("hash_key")) == {"field1", "field2"}
    assert await client.hlen("hash_key") == 2

    assert await client.client.hgetall(client.make_key("hash_key")) == {
        "field1": "value1",
        "field2": "value2",
    }
    # 测试删除操作
    await client.hdel("hash_key", "field1")
    assert not await client.hexists("hash_key", "field1")
    assert not await client.client.hexists(client.make_key("hash_key"), "field1")
    # 清理测试数据
    await client.delete("hash_key")


async def test_hash_special_cases(client) -> None:
    """测试哈希表的特殊情况"""
    # 清理可能存在的测试数据
    await client.delete("hash_key", "empty_hash")

    # 1. 空值测试
    # 测试空哈希表
    assert await client.hlen("empty_hash") == 0
    assert await client.hkeys("empty_hash") == []
    assert await client.client.hlen(client.make_key("empty_hash")) == 0
    assert await client.client.hkeys(client.make_key("empty_hash")) == []
    # 测试空字段值
    await client.hset("hash_key", "empty_field", "")
    assert await client.hget("hash_key", "empty_field") == ""
    assert await client.client.hget(client.make_key("hash_key"), "empty_field") == ""

    # 2. 特殊字符测试
    special_fields = {
        "field:with:colon": "value1",
        "field with space": "value2",
        "field_with_unicode_中文": "value3",
        "": "empty_field",  # 空字段名
        "123": "numeric_field",  # 数字字段名
    }
    await client.hmset("hash_key", special_fields)
    assert await client.hgetall("hash_key") == {**special_fields, "empty_field": ""}

    # 3. 数值操作测试
    # 测试 hincrby 的各种情况
    await client.hset("hash_key", "counter", "10")
    assert await client.hincrby("hash_key", "counter", 5) == 15
    assert await client.hincrby("hash_key", "counter", -3) == 12
    assert await client.hincrby("hash_key", "new_counter", 7) == 7

    # 测试非数字值
    await client.hset("hash_key", "non_number", "abc")
    with pytest.raises(ResponseError):
        await client.hincrby("hash_key", "non_number", 5)

    # 4. 大量数据测试
    large_fields = {f"field{i}": f"value{i}" for i in range(1000)}
    await client.hmset("large_hash", large_fields)
    assert await client.hlen("large_hash") == 1000
    assert set(await client.hkeys("large_hash")) == set(large_fields.keys())

    # 清理测试数据
    await client.delete("hash_key", "empty_hash", "large_hash")


async def test_list_basic_operations(client) -> None:
    """测试列表的基本操作"""
    # 清理可能存在的测试数据
    await client.delete("list_key")

    # 测试从左侧添加元素
    await client.lpush("list_key", "item1", "item2")
    assert await client.lrange("list_key", 0, -1) == ["item2", "item1"]
    assert await client.client.lrange(client.make_key("list_key"), 0, -1) == [
        "item2",
        "item1",
    ]
    # 测试从右侧添加元素
    await client.rpush("list_key", "item3")
    assert await client.lrange("list_key", 0, -1) == ["item2", "item1", "item3"]
    assert await client.client.lrange(client.make_key("list_key"), 0, -1) == [
        "item2",
        "item1",
        "item3",
    ]
    # 测试从左侧弹出元素
    assert await client.lpop("list_key") == "item2"
    assert await client.lrange("list_key", 0, -1) == ["item1", "item3"]
    assert await client.client.lrange(client.make_key("list_key"), 0, -1) == [
        "item1",
        "item3",
    ]
    # 测试从右侧弹出元素
    assert await client.rpop("list_key") == "item3"
    assert await client.lrange("list_key", 0, -1) == ["item1"]
    assert await client.client.lrange(client.make_key("list_key"), 0, -1) == ["item1"]

    # 清理测试数据
    await client.delete("list_key")


async def test_list_special_cases(client) -> None:
    """测试列表的特殊情况"""
    # 清理可能存在的测试数据
    await client.delete("list_key", "empty_list")

    # 1. 空列表测试
    assert await client.lrange("empty_list", 0, -1) == []
    assert await client.lpop("empty_list") is None
    assert await client.rpop("empty_list") is None

    # 2. 特殊字符测试
    special_items = [
        "item:with:colon",
        "item with space",
        "item_with_unicode_中文",
        "",  # 空字符串
        "123",  # 数字字符串
    ]
    await client.rpush("list_key", *special_items)
    assert await client.lrange("list_key", 0, -1) == special_items

    # 3. 范围操作测试
    # 测试正常范围
    assert await client.lrange("list_key", 1, 3) == special_items[1:4]

    # 测试负数索引
    assert await client.lrange("list_key", -3, -1) == special_items[-3:]

    # 测试越界范围
    assert await client.lrange("list_key", 0, 100) == special_items
    assert await client.lrange("list_key", -100, 100) == special_items

    # 4. 大量数据测试
    large_items = [f"item{i}" for i in range(1000)]
    await client.rpush("large_list", *large_items)
    assert len(await client.lrange("large_list", 0, -1)) == 1000
    assert await client.lrange("large_list", 0, 999) == large_items

    # 5. 批量操作测试
    # 批量弹出
    popped_items = []
    for _ in range(5):
        item = await client.lpop("list_key")
        if item is not None:
            popped_items.append(item)
    assert popped_items == special_items

    # 清理测试数据
    await client.delete("list_key", "empty_list", "large_list")


async def test_set_basic_operations(client) -> None:
    """测试集合的基本操作"""
    # 清理可能存在的测试数据
    await client.delete("set_key")

    # 测试添加成员
    await client.sadd("set_key", "member1", "member2", "member3")
    assert await client.smembers("set_key") == {"member1", "member2", "member3"}
    assert await client.client.smembers(client.make_key("set_key")) == {
        "member1",
        "member2",
        "member3",
    }
    # 测试检查成员存在
    assert await client.sismember("set_key", "member1")
    assert await client.client.sismember(client.make_key("set_key"), "member1")
    assert not await client.sismember("set_key", "nonexistent")

    # 测试获取集合大小
    assert await client.scard("set_key") == 3
    assert await client.client.scard(client.make_key("set_key")) == 3
    # 测试删除成员
    await client.srem("set_key", "member1")
    assert not await client.sismember("set_key", "member1")
    assert await client.scard("set_key") == 2
    assert await client.client.scard(client.make_key("set_key")) == 2

    # 清理测试数据
    await client.delete("set_key")


async def test_set_special_cases(client) -> None:
    """测试集合的特殊情况"""
    # 清理可能存在的测试数据
    await client.delete("set_key", "empty_set", "set1", "set2")

    # 1. 空集合测试
    assert await client.smembers("empty_set") == set()
    assert await client.scard("empty_set") == 0
    assert not await client.sismember("empty_set", "any_member")

    # 2. 特殊字符成员测试
    special_members = {
        "member:with:colon",
        "member with space",
        "member_with_unicode_中文",
        "",  # 空成员
        "123",  # 数字成员
    }
    await client.sadd("set_key", *special_members)
    assert await client.smembers("set_key") == special_members

    # 3. 重复添加测试
    initial_size = await client.scard("set_key")
    await client.sadd("set_key", "member:with:colon")  # 添加已存在的成员
    assert await client.scard("set_key") == initial_size

    # 4. 大量成员测试
    large_members = {f"member{i}" for i in range(1000)}
    await client.sadd("large_set", *large_members)
    assert await client.scard("large_set") == 1000
    assert await client.smembers("large_set") == large_members

    # 5. 集合运算测试
    # 准备两个集合并确保它们是空的
    await client.delete("set1", "set2")
    await client.sadd("set1", "a", "b", "c")
    await client.sadd("set2", "b", "c", "d")
    await client.sadd("set3", "b")
    # 交集
    result = await client.sinter("set1", "set2")
    assert result == {"b", "c"}
    result = await client.sinter("set1", "set2", "set3")
    assert result == {"b"}
    # 并集
    result = await client.sunion("set1", "set2")
    assert result == {"a", "b", "c", "d"}
    result = await client.sunion("set1", "set2", "set3")
    assert result == {"a", "b", "c", "d"}

    # 差集
    result = await client.sdiff("set1", "set2")
    assert result == {"a"}
    result = await client.sdiff("set2", "set1")
    assert result == {"d"}
    # 清理测试数据
    await client.delete("set_key", "empty_set", "large_set", "set1", "set2")


async def test_zset_basic_operations(client) -> None:
    """测试有序集合的基本操作"""
    # 清理可能存在的测试数据
    await client.delete("zset_key")

    # 测试添加成员
    members = {"member1": 1.0, "member2": 2.0, "member3": 3.0}
    await client.zadd("zset_key", members)

    # 测试获取分数
    assert await client.zscore("zset_key", "member2") == 2.0
    assert await client.client.zscore(client.make_key("zset_key"), "member2") == 2.0
    # 测试获取排名
    assert await client.zrank("zset_key", "member1") == 0
    assert await client.client.zrank(client.make_key("zset_key"), "member1") == 0

    assert await client.zrevrank("zset_key", "member3") == 0
    assert await client.client.zrevrank(client.make_key("zset_key"), "member3") == 0

    # 测试计数
    assert await client.zcard("zset_key") == 3
    assert await client.client.zcard(client.make_key("zset_key")) == 3

    assert await client.zcount("zset_key", 1.0, 2.0) == 2
    assert await client.client.zcount(client.make_key("zset_key"), 1.0, 2.0) == 2

    # 测试范围查询
    assert await client.zrange("zset_key", 0, -1) == ["member1", "member2", "member3"]
    assert await client.client.zrange(client.make_key("zset_key"), 0, -1) == [
        "member1",
        "member2",
        "member3",
    ]

    assert await client.zrevrange("zset_key", 0, -1) == [
        "member3",
        "member2",
        "member1",
    ]
    assert await client.client.zrevrange(client.make_key("zset_key"), 0, -1) == [
        "member3",
        "member2",
        "member1",
    ]

    # 测试带分数的范围查询
    result = await client.zrevrange("zset_key", 0, -1, withscores=True)
    assert result == [("member3", 3.0), ("member2", 2.0), ("member1", 1.0)]
    result = await client.client.zrevrange(
        client.make_key("zset_key"), 0, -1, withscores=True
    )
    assert result == [("member3", 3.0), ("member2", 2.0), ("member1", 1.0)]

    # 清理测试数据
    await client.delete("zset_key")


async def test_zset_special_cases(client) -> None:
    """测试有序集合的特殊情况"""
    # 清理可能存在的测试数据
    await client.delete("zset_key", "empty_zset")

    # 1. 空有序集合测试
    assert await client.zcard("empty_zset") == 0
    assert await client.zrange("empty_zset", 0, -1) == []
    assert await client.zscore("empty_zset", "any_member") is None

    # 2. 特殊分数测试
    special_scores = {
        "negative": -1.0,
        "zero": 0.0,
        "positive": 1.0,
        "float": 3.14,
        "large": 1e308,
        "small": 1e-308,
    }
    await client.zadd("zset_key", special_scores)
    for member, score in special_scores.items():
        assert await client.zscore("zset_key", member) == score

    # 3. 分数更新测试
    await client.zadd("zset_key", {"member": 1.0})
    await client.zadd("zset_key", {"member": 2.0})
    assert await client.zscore("zset_key", "member") == 2.0

    # 4. 范围查询测试
    # 测试开区间
    count = await client.zcount("zset_key", "(0", "2")
    members = await client.zrangebyscore("zset_key", "(0", "2")
    assert len(members) == count

    # 测试闭区间
    count = await client.zcount("zset_key", "0", "2")
    members = await client.zrangebyscore("zset_key", "0", "2")
    assert len(members) == count

    # 5. 删除测试
    # 按排名删除
    await client.zremrangebyrank("zset_key", 0, 1)

    # 按分数删除
    await client.zremrangebyscore("zset_key", 2.0, 3.0)

    # 6. 增量操作测试
    await client.zadd("zset_key", {"counter": 1.0})
    assert await client.zincrby("zset_key", 2.0, "counter") == 3.0
    assert await client.zincrby("zset_key", -1.0, "counter") == 2.0

    # 清理测试数据
    await client.delete("zset_key", "empty_zset")


async def test_zset_range_operations(client) -> None:
    """测试有序集合的范围操作"""
    # 清理可能存在的测试数据
    await client.delete("zset_key")

    # 准备测试数据
    test_data = {
        "member1": 1.0,
        "member2": 2.0,
        "member3": 3.0,
        "member4": 4.0,
        "member5": 5.0,
    }
    await client.zadd("zset_key", test_data)

    # 测试正向范围查询
    assert await client.zrange("zset_key", 0, -1) == [
        "member1",
        "member2",
        "member3",
        "member4",
        "member5",
    ]
    assert await client.client.zrange(client.make_key("zset_key"), 0, -1) == [
        "member1",
        "member2",
        "member3",
        "member4",
        "member5",
    ]

    # 测试反向范围查询
    assert await client.zrevrange("zset_key", 0, -1) == [
        "member5",
        "member4",
        "member3",
        "member2",
        "member1",
    ]
    assert await client.client.zrevrange(client.make_key("zset_key"), 0, -1) == [
        "member5",
        "member4",
        "member3",
        "member2",
        "member1",
    ]

    # 测试部分范围
    assert await client.zrange("zset_key", 1, 3) == ["member2", "member3", "member4"]
    assert await client.client.zrange(client.make_key("zset_key"), 1, 3) == [
        "member2",
        "member3",
        "member4",
    ]
    # 测试部分范围（反向）
    assert await client.zrevrange("zset_key", 1, 3) == ["member4", "member3", "member2"]
    assert await client.client.zrevrange(client.make_key("zset_key"), 1, 3) == [
        "member4",
        "member3",
        "member2",
    ]
    # 测试带分数的反向范围查询
    result = await client.zrevrange("zset_key", 0, -1, withscores=True)
    expected = [
        ("member5", 5.0),
        ("member4", 4.0),
        ("member3", 3.0),
        ("member2", 2.0),
        ("member1", 1.0),
    ]
    assert result == expected

    # 清理测试数据
    await client.delete("zset_key")


# 添加计数器操作的专门测试用例
async def test_counter_basic_operations(client) -> None:
    """测试计数器的基本操作"""
    # 清理可能存在的测试数据
    await client.delete("counter")

    # 测试自增操作
    assert await client.incr("counter") == 1
    assert await client.incrby("counter", 5) == 6
    assert await client.get("counter") == "6"

    # 测试自减操作
    assert await client.decr("counter") == 5
    assert await client.decrby("counter", 2) == 3
    assert await client.get("counter") == "3"
    # 清理测试数据
    await client.delete("counter")


async def test_counter_special_cases(client) -> None:
    """测试计数器的特殊情况"""
    # 清理可能存在的测试数据
    await client.delete("counter")

    # 1. 对不存在的键进行操作
    assert await client.incr("counter") == 1
    await client.delete("counter")
    assert await client.decr("counter") == -1

    # 2. 大数值测试
    await client.set("counter", "1000000")
    assert await client.incrby("counter", 1000000) == 2000000
    assert await client.decrby("counter", 2000000) == 0

    # 3. 负数测试
    assert await client.decrby("counter", 100) == -100
    assert await client.incrby("counter", 100) == 0

    # 4. 非数字值测试
    await client.set("counter", "abc")
    with pytest.raises(ResponseError):
        await client.incr("counter")

    # 清理测试数据
    await client.delete("counter")


# 添加管道操作的测试用例
async def test_pipeline_operations(client) -> None:
    """测试管道操作"""
    # 清理可能存在的测试数据
    await client.delete("key1", "key2", "key3")

    # 创建管道
    pipe = await client.pipeline()

    # 添加命令到管道
    pipe.set(client.make_key("key1"), "value1")
    pipe.set(client.make_key("key2"), "value2")
    pipe.set(client.make_key("key3"), "value3")

    # 执行管道
    await pipe.execute()

    # 验证结果
    assert await client.get("key1") == "value1"
    assert await client.get("key2") == "value2"
    assert await client.get("key3") == "value3"

    # 测试管道中的错误处理
    pipe = await client.pipeline()
    pipe.set(client.make_key("key1"), "new_value")
    pipe.incr(client.make_key("key1"))  # 这会失败，因为 key1 的值不是数字
    pipe.set(client.make_key("key2"), "value2")

    try:
        await pipe.execute()
    except ResponseError:
        # 验证部分命令是否执行成功
        assert await client.get("key1") == "new_value"
        assert await client.get("key2") == "value2"

    # 测试复杂命令
    pipe = await client.pipeline()
    pipe.hset(client.make_key("hash_key"), "field1", "value1")
    pipe.zadd(client.make_key("zset_key"), {"member1": 1.0})
    pipe.sadd(client.make_key("set_key"), "member1")
    pipe.lpush(client.make_key("list_key"), "item1")
    await pipe.execute()

    # 验证复杂命令结果
    assert await client.hget("hash_key", "field1") == "value1"
    assert await client.zscore("zset_key", "member1") == 1.0
    assert await client.sismember("set_key", "member1")
    assert await client.lrange("list_key", 0, -1) == ["item1"]

    # 清理测试数据
    await client.delete(
        "key1", "key2", "key3", "hash_key", "zset_key", "set_key", "list_key"
    )
