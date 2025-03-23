import os
import pytest
import asyncio
from unfazed_redis.backends.namespaceclient import NamespaceClient
from redis.exceptions import ResponseError

HOST = os.getenv("REDIS_HOST", "redis")


@pytest.fixture(scope="session")
def event_loop():
    """Create a session-level event loop"""
    policy = asyncio.get_event_loop_policy()
    loop = policy.new_event_loop()
    yield loop
    loop.close()


@pytest.fixture(scope="function")
async def client(event_loop):
    """Create a test Redis client"""
    client = NamespaceClient(
        f"redis://{HOST}:6379",
        options={"decode_responses": True, "PREFIX": "test", "VERSION": "1"},
    )

    # Clean up any existing test data
    await client.delete("foo", "hash_key", "list_key", "set_key", "zset_key", "counter")

    yield client

    # Clean up test data after test
    await client.delete("foo", "hash_key", "list_key", "set_key", "zset_key", "counter")


# First part: Basic configuration test
async def test_client_basic_initialization(client) -> None:
    """Test client basic initialization"""
    # Test default initialization
    client = NamespaceClient(
        location=f"redis://{HOST}:6379", options={"PREFIX": "test", "VERSION": "11"}
    )

    # Test basic key prefix
    key = client.make_key("test_key")
    assert key == f"{client.options.prefix}:{client.options.version}:test_key"


async def test_client_custom_initialization(client) -> None:
    """Test client custom configuration initialization"""
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

    # Verify configuration
    assert client.options.decode_responses
    assert client.options.prefix == "custom"
    assert client.options.version == "2"
    assert client.options.retry
    assert client.options.socket_timeout == 5.0

    # Verify key prefix
    key = client.make_key("test_key")
    assert key == "custom:2:test_key"


# Second part: Basic key value operation test
async def test_key_basic_operations(client) -> None:
    """Test basic key value operations"""
    # Test set and get
    await client.set("basic_key", "value1")
    assert await client.get("basic_key") == "value1"
    assert await client.client.get(client.make_key("basic_key")) == "value1"
    # Test delete
    await client.delete("basic_key")
    assert await client.exists("basic_key") == 0


async def test_key_edge_cases(client) -> None:
    """Test key value operations edge cases"""
    # Test non-existent key
    assert await client.get("nonexistent_key") is None

    # Test empty value
    await client.set("empty_key", "")
    assert await client.get("empty_key") == ""

    # Test special character keys
    special_keys = ["key:with:colon", "key with space", "key_with_unicode_中文"]
    for key in special_keys:
        await client.set(key, "value")
        assert await client.get(key) == "value"
        await client.delete(key)


async def test_key_expiration_operations(client) -> None:
    """Test key expiration operations"""
    # Test setting expiration time
    await client.set("expire_key", "value", timeout=1)
    ttl = await client.ttl("expire_key")
    assert 0 < ttl <= 1

    ttl = await client.client.ttl(client.make_key("expire_key"))
    assert 0 < ttl <= 1

    # Test permanent key
    await client.set("permanent_key", "value")
    assert await client.ttl("permanent_key") == -1
    assert await client.client.ttl(client.make_key("permanent_key")) == -1
    # Test updating expiration time
    await client.expire("permanent_key", 5)
    assert 0 < await client.ttl("permanent_key") <= 5
    assert 0 < await client.client.ttl(client.make_key("permanent_key")) <= 5


async def test_hash_basic_operations(client) -> None:
    """Test basic hash operations"""
    # Prepare test data
    await client.hmset("hash_key", {"field1": "value1", "field2": "value2"})

    # Test basic CRUD operations
    assert await client.hget("hash_key", "field1") == "value1"
    assert await client.hgetall("hash_key") == {"field1": "value1", "field2": "value2"}
    assert await client.hexists("hash_key", "field1")
    assert set(await client.hkeys("hash_key")) == {"field1", "field2"}
    assert await client.hlen("hash_key") == 2

    assert await client.client.hgetall(client.make_key("hash_key")) == {
        "field1": "value1",
        "field2": "value2",
    }
    # Test delete operation
    await client.hdel("hash_key", "field1")
    assert not await client.hexists("hash_key", "field1")
    assert not await client.client.hexists(client.make_key("hash_key"), "field1")
    # Clean up test data
    await client.delete("hash_key")


async def test_hash_special_cases(client) -> None:
    """Test special cases for hash operations"""
    # Clean up any existing test data
    await client.delete("hash_key", "empty_hash")

    # 1. Empty value test
    # Test empty hash
    assert await client.hlen("empty_hash") == 0
    assert await client.hkeys("empty_hash") == []
    assert await client.client.hlen(client.make_key("empty_hash")) == 0
    assert await client.client.hkeys(client.make_key("empty_hash")) == []
    # Test empty field value
    await client.hset("hash_key", "empty_field", "")
    assert await client.hget("hash_key", "empty_field") == ""
    assert await client.client.hget(client.make_key("hash_key"), "empty_field") == ""

    # 2. Special character test
    special_fields = {
        "field:with:colon": "value1",
        "field with space": "value2",
        "field_with_unicode_中文": "value3",
        "": "empty_field",  # Empty field name
        "123": "numeric_field",  # Numeric field name
    }
    await client.hmset("hash_key", special_fields)
    assert await client.hgetall("hash_key") == {**special_fields, "empty_field": ""}

    # 3. Numeric operation test
    # Test hincrby various cases
    await client.hset("hash_key", "counter", "10")
    assert await client.hincrby("hash_key", "counter", 5) == 15
    assert await client.hincrby("hash_key", "counter", -3) == 12
    assert await client.hincrby("hash_key", "new_counter", 7) == 7

    # Test non-numeric value
    await client.hset("hash_key", "non_number", "abc")
    with pytest.raises(ResponseError):
        await client.hincrby("hash_key", "non_number", 5)

    # 4. Large data test
    large_fields = {f"field{i}": f"value{i}" for i in range(1000)}
    await client.hmset("large_hash", large_fields)
    assert await client.hlen("large_hash") == 1000
    assert set(await client.hkeys("large_hash")) == set(large_fields.keys())

    # Clean up test data
    await client.delete("hash_key", "empty_hash", "large_hash")


async def test_list_basic_operations(client) -> None:
    """Test basic list operations"""
    # Clean up any existing test data
    await client.delete("list_key")

    # Test adding elements from left
    await client.lpush("list_key", "item1", "item2")
    assert await client.lrange("list_key", 0, -1) == ["item2", "item1"]
    assert await client.client.lrange(client.make_key("list_key"), 0, -1) == [
        "item2",
        "item1",
    ]
    # Test adding elements from right
    await client.rpush("list_key", "item3")
    assert await client.lrange("list_key", 0, -1) == ["item2", "item1", "item3"]
    assert await client.client.lrange(client.make_key("list_key"), 0, -1) == [
        "item2",
        "item1",
        "item3",
    ]
    # Test popping elements from left
    assert await client.lpop("list_key") == "item2"
    assert await client.lrange("list_key", 0, -1) == ["item1", "item3"]
    assert await client.client.lrange(client.make_key("list_key"), 0, -1) == [
        "item1",
        "item3",
    ]
    # Test popping elements from right
    assert await client.rpop("list_key") == "item3"
    assert await client.lrange("list_key", 0, -1) == ["item1"]
    assert await client.client.lrange(client.make_key("list_key"), 0, -1) == ["item1"]

    # Clean up test data
    await client.delete("list_key")


async def test_list_special_cases(client) -> None:
    """Test special cases for list operations"""
    # Clean up any existing test data
    await client.delete("list_key", "empty_list")

    # 1. Empty list test
    assert await client.lrange("empty_list", 0, -1) == []
    assert await client.lpop("empty_list") is None
    assert await client.rpop("empty_list") is None

    # 2. Special character test
    special_items = [
        "item:with:colon",
        "item with space",
        "item_with_unicode_中文",
        "",  # Empty string
        "123",  # Numeric string
    ]
    await client.rpush("list_key", *special_items)
    assert await client.lrange("list_key", 0, -1) == special_items

    # 3. Range operation test
    # Test normal range
    assert await client.lrange("list_key", 1, 3) == special_items[1:4]

    # Test negative index
    assert await client.lrange("list_key", -3, -1) == special_items[-3:]

    # Test out-of-range range
    assert await client.lrange("list_key", 0, 100) == special_items
    assert await client.lrange("list_key", -100, 100) == special_items

    # 4. Large data test
    large_items = [f"item{i}" for i in range(1000)]
    await client.rpush("large_list", *large_items)
    assert len(await client.lrange("large_list", 0, -1)) == 1000
    assert await client.lrange("large_list", 0, 999) == large_items

    # 5. Batch operation test
    # Batch pop
    popped_items = []
    for _ in range(5):
        item = await client.lpop("list_key")
        if item is not None:
            popped_items.append(item)
    assert popped_items == special_items

    # Clean up test data
    await client.delete("list_key", "empty_list", "large_list")


async def test_set_basic_operations(client) -> None:
    """Test basic set operations"""
    # Clean up any existing test data
    await client.delete("set_key")

    # Test adding members
    await client.sadd("set_key", "member1", "member2", "member3")
    assert await client.smembers("set_key") == {"member1", "member2", "member3"}
    assert await client.client.smembers(client.make_key("set_key")) == {
        "member1",
        "member2",
        "member3",
    }
    # Test checking member existence
    assert await client.sismember("set_key", "member1")
    assert await client.client.sismember(client.make_key("set_key"), "member1")
    assert not await client.sismember("set_key", "nonexistent")

    # Test getting set size
    assert await client.scard("set_key") == 3
    assert await client.client.scard(client.make_key("set_key")) == 3
    # Test removing member
    await client.srem("set_key", "member1")
    assert not await client.sismember("set_key", "member1")
    assert await client.scard("set_key") == 2
    assert await client.client.scard(client.make_key("set_key")) == 2

    # Clean up test data
    await client.delete("set_key")


async def test_set_special_cases(client) -> None:
    """Test special cases for set operations"""
    # Clean up any existing test data
    await client.delete("set_key", "empty_set", "set1", "set2")

    # 1. Empty set test
    assert await client.smembers("empty_set") == set()
    assert await client.scard("empty_set") == 0
    assert not await client.sismember("empty_set", "any_member")

    # 2. Special character member test
    special_members = {
        "member:with:colon",
        "member with space",
        "member_with_unicode_中文",
        "",  # Empty member
        "123",  # Numeric member
    }
    await client.sadd("set_key", *special_members)
    assert await client.smembers("set_key") == special_members

    # 3. Duplicate addition test
    initial_size = await client.scard("set_key")
    await client.sadd("set_key", "member:with:colon")  # Add existing member
    assert await client.scard("set_key") == initial_size

    # 4. Large member test
    large_members = {f"member{i}" for i in range(1000)}
    await client.sadd("large_set", *large_members)
    assert await client.scard("large_set") == 1000
    assert await client.smembers("large_set") == large_members

    # 5. Set operation test
    # Prepare two sets and ensure they are empty
    await client.delete("set1", "set2")
    await client.sadd("set1", "a", "b", "c")
    await client.sadd("set2", "b", "c", "d")
    await client.sadd("set3", "b")
    # Intersection
    result = await client.sinter("set1", "set2")
    assert result == {"b", "c"}
    result = await client.sinter("set1", "set2", "set3")
    assert result == {"b"}
    # Union
    result = await client.sunion("set1", "set2")
    assert result == {"a", "b", "c", "d"}
    result = await client.sunion("set1", "set2", "set3")
    assert result == {"a", "b", "c", "d"}

    # Difference
    result = await client.sdiff("set1", "set2")
    assert result == {"a"}
    result = await client.sdiff("set2", "set1")
    assert result == {"d"}
    # Clean up test data
    await client.delete("set_key", "empty_set", "large_set", "set1", "set2")


async def test_zset_basic_operations(client) -> None:
    """Test basic zset operations"""
    # Clean up any existing test data
    await client.delete("zset_key")

    # Test adding members
    members = {"member1": 1.0, "member2": 2.0, "member3": 3.0}
    await client.zadd("zset_key", members)

    # Test getting score
    assert await client.zscore("zset_key", "member2") == 2.0
    assert await client.client.zscore(client.make_key("zset_key"), "member2") == 2.0
    # Test getting rank
    assert await client.zrank("zset_key", "member1") == 0
    assert await client.client.zrank(client.make_key("zset_key"), "member1") == 0

    assert await client.zrevrank("zset_key", "member3") == 0
    assert await client.client.zrevrank(client.make_key("zset_key"), "member3") == 0

    # Test counting
    assert await client.zcard("zset_key") == 3
    assert await client.client.zcard(client.make_key("zset_key")) == 3

    assert await client.zcount("zset_key", 1.0, 2.0) == 2
    assert await client.client.zcount(client.make_key("zset_key"), 1.0, 2.0) == 2

    # Test range query
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

    # Test range query with scores
    result = await client.zrevrange("zset_key", 0, -1, withscores=True)
    assert result == [("member3", 3.0), ("member2", 2.0), ("member1", 1.0)]
    result = await client.client.zrevrange(
        client.make_key("zset_key"), 0, -1, withscores=True
    )
    assert result == [("member3", 3.0), ("member2", 2.0), ("member1", 1.0)]

    # Clean up test data
    await client.delete("zset_key")


async def test_zset_special_cases(client) -> None:
    """Test special cases for zset operations"""
    # Clean up any existing test data
    await client.delete("zset_key", "empty_zset")

    # 1. Empty zset test
    assert await client.zcard("empty_zset") == 0
    assert await client.zrange("empty_zset", 0, -1) == []
    assert await client.zscore("empty_zset", "any_member") is None

    # 2. Special score test
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

    # 3. Score update test
    await client.zadd("zset_key", {"member": 1.0})
    await client.zadd("zset_key", {"member": 2.0})
    assert await client.zscore("zset_key", "member") == 2.0

    # 4. Range query test
    # Test open interval
    count = await client.zcount("zset_key", "(0", "2")
    members = await client.zrangebyscore("zset_key", "(0", "2")
    assert len(members) == count

    # Test closed interval
    count = await client.zcount("zset_key", "0", "2")
    members = await client.zrangebyscore("zset_key", "0", "2")
    assert len(members) == count

    # 5. Delete test
    # Delete by rank
    await client.zremrangebyrank("zset_key", 0, 1)

    # Delete by score
    await client.zremrangebyscore("zset_key", 2.0, 3.0)

    # 6. Increment operation test
    await client.zadd("zset_key", {"counter": 1.0})
    assert await client.zincrby("zset_key", 2.0, "counter") == 3.0
    assert await client.zincrby("zset_key", -1.0, "counter") == 2.0

    # Clean up test data
    await client.delete("zset_key", "empty_zset")


async def test_zset_range_operations(client) -> None:
    """Test range operations for zset"""
    # Clean up any existing test data
    await client.delete("zset_key")

    # Prepare test data
    test_data = {
        "member1": 1.0,
        "member2": 2.0,
        "member3": 3.0,
        "member4": 4.0,
        "member5": 5.0,
    }
    await client.zadd("zset_key", test_data)

    # Test forward range query
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

    # Test backward range query
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

    # Test partial range
    assert await client.zrange("zset_key", 1, 3) == ["member2", "member3", "member4"]
    assert await client.client.zrange(client.make_key("zset_key"), 1, 3) == [
        "member2",
        "member3",
        "member4",
    ]
    # Test partial range (backward)
    assert await client.zrevrange("zset_key", 1, 3) == ["member4", "member3", "member2"]
    assert await client.client.zrevrange(client.make_key("zset_key"), 1, 3) == [
        "member4",
        "member3",
        "member2",
    ]
    # Test backward range query with scores
    result = await client.zrevrange("zset_key", 0, -1, withscores=True)
    expected = [
        ("member5", 5.0),
        ("member4", 4.0),
        ("member3", 3.0),
        ("member2", 2.0),
        ("member1", 1.0),
    ]
    assert result == expected

    # Clean up test data
    await client.delete("zset_key")


# Add counter operation specific test cases
async def test_counter_basic_operations(client) -> None:
    """Test basic counter operations"""
    # Clean up any existing test data
    await client.delete("counter")

    # Test increment operation
    assert await client.incr("counter") == 1
    assert await client.incrby("counter", 5) == 6
    assert await client.get("counter") == "6"

    # Test decrement operation
    assert await client.decr("counter") == 5
    assert await client.decrby("counter", 2) == 3
    assert await client.get("counter") == "3"
    # Clean up test data
    await client.delete("counter")


async def test_counter_special_cases(client) -> None:
    """Test special cases for counter operations"""
    # Clean up any existing test data
    await client.delete("counter")

    # 1. Operations on non-existent keys
    assert await client.incr("counter") == 1
    await client.delete("counter")
    assert await client.decr("counter") == -1

    # 2. Large number test
    await client.set("counter", "1000000")
    assert await client.incrby("counter", 1000000) == 2000000
    assert await client.decrby("counter", 2000000) == 0

    # 3. Negative number test
    assert await client.decrby("counter", 100) == -100
    assert await client.incrby("counter", 100) == 0

    # 4. Non-numeric value test
    await client.set("counter", "abc")
    with pytest.raises(ResponseError):
        await client.incr("counter")

    # Clean up test data
    await client.delete("counter")


# Add pipeline operation test cases
async def test_pipeline_operations(client) -> None:
    """Test pipeline operations"""
    # Clean up any existing test data
    await client.delete("key1", "key2", "key3")

    # Create pipeline
    pipe = await client.pipeline()

    # Add commands to pipeline
    pipe.set(client.make_key("key1"), "value1")
    pipe.set(client.make_key("key2"), "value2")
    pipe.set(client.make_key("key3"), "value3")

    # Execute pipeline
    await pipe.execute()

    # Verify results
    assert await client.get("key1") == "value1"
    assert await client.get("key2") == "value2"
    assert await client.get("key3") == "value3"

    # Test error handling in pipeline
    pipe = await client.pipeline()
    pipe.set(client.make_key("key1"), "new_value")
    pipe.incr(client.make_key("key1"))  # This will fail as key1's value is not numeric
    pipe.set(client.make_key("key2"), "value2")

    try:
        await pipe.execute()
    except ResponseError:
        # Verify partial command execution
        assert await client.get("key1") == "new_value"
        assert await client.get("key2") == "value2"

    # Test complex commands
    pipe = await client.pipeline()
    pipe.hset(client.make_key("hash_key"), "field1", "value1")
    pipe.zadd(client.make_key("zset_key"), {"member1": 1.0})
    pipe.sadd(client.make_key("set_key"), "member1")
    pipe.lpush(client.make_key("list_key"), "item1")
    await pipe.execute()

    # Verify complex command results
    assert await client.hget("hash_key", "field1") == "value1"
    assert await client.zscore("zset_key", "member1") == 1.0
    assert await client.sismember("set_key", "member1")
    assert await client.lrange("list_key", 0, -1) == ["item1"]

    # Clean up test data
    await client.delete(
        "key1", "key2", "key3", "hash_key", "zset_key", "set_key", "list_key"
    )
