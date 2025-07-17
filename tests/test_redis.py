import asyncio
import os
import typing as t

import pytest
from redis.asyncio.retry import Retry
from redis.backoff import ExponentialBackoff
from redis.exceptions import DataError, ResponseError

from unfazed_redis.backends.namespaceclient import NamespaceClient

HOST = os.getenv("REDIS_HOST", "redis")


@pytest.fixture(scope="session")
def event_loop() -> t.Generator[asyncio.AbstractEventLoop, None, None]:
    """Create a session-level event loop"""
    policy = asyncio.get_event_loop_policy()
    loop = policy.new_event_loop()
    yield loop
    loop.close()


@pytest.fixture(scope="function")
async def client(
    event_loop: asyncio.AbstractEventLoop,
) -> t.AsyncGenerator[NamespaceClient, None]:
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
async def test_client_basic_initialization(client: NamespaceClient) -> None:
    """Test client basic initialization"""
    # Test default initialization
    client = NamespaceClient(
        location=f"redis://{HOST}:6379", options={"PREFIX": "test", "VERSION": "11"}
    )

    # Test basic key prefix
    key = client.make_key("test_key")
    assert key == f"{client.options.prefix}:{client.options.version}:test_key"

    # Test retry is None
    client2 = NamespaceClient(
        location=f"redis://{HOST}:6379",
        options={"PREFIX": "test", "VERSION": "11", "retry": None},
    )
    assert client2.options.retry is None


async def test_client_custom_initialization(client: NamespaceClient) -> None:
    """Test client custom configuration initialization"""
    custom_options = {
        "decode_responses": True,
        "PREFIX": "custom",
        "VERSION": "2",
        "retry": Retry(ExponentialBackoff(), 3),
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
async def test_key_basic_operations(client: NamespaceClient) -> None:
    """Test basic key value operations"""
    # Test set and get
    await client.set("basic_key", "value1")
    assert await client.get("basic_key") == "value1"
    assert await client.client.get(client.make_key("basic_key")) == "value1"
    # Test delete
    await client.delete("basic_key")
    assert await client.exists("basic_key") == 0


async def test_key_edge_cases(client: NamespaceClient) -> None:
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


async def test_key_expiration_operations(client: NamespaceClient) -> None:
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


async def test_hash_basic_operations(client: NamespaceClient) -> None:
    """Test basic hash operations"""
    # Prepare test data
    await client.hmset("hash_key", {"field1": "value1", "field2": "value2"})

    # Test basic CRUD operations
    assert await client.hget("hash_key", "field1") == "value1"
    assert await client.hgetall("hash_key") == {"field1": "value1", "field2": "value2"}

    # Test hmget operation
    assert await client.hmget("hash_key", ["field1", "field2"]) == ["value1", "value2"]
    assert await client.hmget("hash_key", ["field1", "nonexistent"]) == ["value1", None]
    assert await t.cast(
        t.Awaitable[list],
        client.client.hmget(client.make_key("hash_key"), ["field1", "field2"]),
    ) == ["value1", "value2"]

    assert await client.hexists("hash_key", "field1")
    assert set(await client.hkeys("hash_key")) == {"field1", "field2"}
    assert await client.hlen("hash_key") == 2

    assert await t.cast(
        t.Awaitable[dict], client.client.hgetall(client.make_key("hash_key"))
    ) == {
        "field1": "value1",
        "field2": "value2",
    }
    # Test delete operation
    await client.hdel("hash_key", "field1")
    assert not await client.hexists("hash_key", "field1")
    assert not await t.cast(
        t.Awaitable[bool], client.client.hexists(client.make_key("hash_key"), "field1")
    )
    # Clean up test data
    await client.delete("hash_key")


async def test_hash_special_cases(client: NamespaceClient) -> None:
    """Test special cases for hash operations"""
    # Clean up any existing test data
    await client.delete("hash_key", "empty_hash")

    # 1. Empty value test
    # Test empty hash
    assert await client.hlen("empty_hash") == 0
    assert await client.hkeys("empty_hash") == []
    assert (
        await t.cast(
            t.Awaitable[int], client.client.hlen(client.make_key("empty_hash"))
        )
        == 0
    )
    assert (
        await t.cast(
            t.Awaitable[list], client.client.hkeys(client.make_key("empty_hash"))
        )
        == []
    )
    # Test empty field value
    await client.hset("hash_key", "empty_field", "")
    assert await client.hget("hash_key", "empty_field") == ""
    assert (
        await t.cast(
            t.Awaitable[t.Optional[str]],
            client.client.hget(client.make_key("hash_key"), "empty_field"),
        )
        == ""
    )

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

    # Test hmget with special cases
    result = await client.hmget("hash_key", ["field:with:colon", "nonexistent", "123"])
    assert result == ["value1", None, "numeric_field"]

    # Test hmget with empty list
    assert await client.hmget("hash_key", []) == []

    # Test hmget with None or empty values
    await client.hset("hash_key", "empty_field", "")
    result = await client.hmget("hash_key", ["empty_field"])
    assert result == [""]

    # Test hmget on non-existent hash
    assert await client.hmget("nonexistent_hash", ["field1", "field2"]) == [None, None]

    # Test hmget with all special characters
    special_fields_list = list(special_fields.keys())
    result = await client.hmget("hash_key", special_fields_list)
    assert result == list(special_fields.values())

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

    # Test hmset parameter validation
    with pytest.raises(DataError, match="'hmset' value must be a dict"):
        await client.hmset("hash_key", "not_a_dict")  # type: ignore

    with pytest.raises(DataError, match="'hmset' value must be a dict"):
        await client.hmset("hash_key", ["also_not_a_dict"])  # type: ignore

    # Test empty dict - should not raise error and do nothing
    await client.hmset("hash_key", {})

    # Verify the data is unchanged, including all fields that were added during the test
    expected_data = {
        **special_fields,
        "empty_field": "",
        "counter": "12",
        "new_counter": "7",
        "non_number": "abc",
    }
    assert await client.hgetall("hash_key") == expected_data

    # Test hsetnx operations
    # Test setting new field
    assert await client.hsetnx("hash_key", "new_field", "new_value") == 1
    assert await client.hget("hash_key", "new_field") == "new_value"

    # Test attempting to set existing field
    assert await client.hsetnx("hash_key", "new_field", "another_value") == 0
    assert (
        await client.hget("hash_key", "new_field") == "new_value"
    )  # Value should remain unchanged

    # Test with special characters
    assert (
        await client.hsetnx("hash_key", "field:with:colon", "new_value") == 0
    )  # Already exists
    assert (
        await client.hsetnx("hash_key", "新字段", "新值") == 1
    )  # New field with Unicode
    assert await client.hget("hash_key", "新字段") == "新值"

    # Test with empty value
    assert await client.hsetnx("hash_key", "empty_value_field", "") == 1
    assert await client.hget("hash_key", "empty_value_field") == ""

    # Update expected data for final verification
    expected_data.update(
        {
            "new_field": "new_value",
            "新字段": "新值",
            "empty_value_field": "",
        }
    )
    assert await client.hgetall("hash_key") == expected_data

    # Test hvals operations
    # Test basic hvals
    values = await client.hvals("hash_key")
    assert set(values) == {
        "value1",  # from field:with:colon
        "value2",  # from field with space
        "value3",  # from field_with_unicode_中文
        "empty_field",  # from empty field name
        "numeric_field",  # from 123
        "",  # from empty_field
        "12",  # from counter
        "7",  # from new_counter
        "abc",  # from non_number
        "new_value",  # from new_field
        "新值",  # from empty_value_field
    }

    # Test hvals on empty hash
    assert await client.hvals("empty_hash") == []

    # Test hvals on non-existent hash
    assert await client.hvals("nonexistent_hash") == []

    # Test hvals on hash with only empty values
    await client.hmset("empty_values_hash", {"field1": "", "field2": ""})
    assert set(await client.hvals("empty_values_hash")) == {""}

    # Clean up additional test data
    await client.delete("empty_values_hash")

    # Test hscan operations
    # Prepare test data with a larger dataset for scanning
    scan_data = {
        f"scan_field_{i}": f"value_{i}"
        for i in range(100)  # Create 100 fields for testing scan
    }
    await client.hmset("scan_hash", scan_data)

    # Test basic scan
    cursor = 0
    all_fields = {}
    while True:
        cursor, pairs = await client.hscan("scan_hash", cursor)
        all_fields.update(pairs)
        if cursor == 0:  # Iteration complete
            break
    assert all_fields == scan_data

    # Test scan with match pattern
    cursor = 0
    matched_fields = {}
    while True:
        cursor, pairs = await client.hscan("scan_hash", cursor, match="scan_field_1*")
        matched_fields.update(pairs)
        if cursor == 0:
            break
    # Should match scan_field_1, scan_field_10-19, scan_field_100
    assert all(k.startswith("scan_field_1") for k in matched_fields.keys())

    # Test scan with count
    cursor = 0
    first_batch = {}
    cursor, pairs = await client.hscan("scan_hash", cursor, count=10)
    first_batch.update(pairs)
    assert len(first_batch) > 0  # Should return some items
    # Note: Even with small count, Redis might return all items in one iteration
    # So we only verify that we got some data, not the cursor value

    # Test scan with count in multiple iterations
    all_data = {}
    iterations = 0
    cursor = 0
    while True:
        cursor, pairs = await client.hscan("scan_hash", cursor, count=10)
        iterations += 1
        all_data.update(pairs)
        if cursor == 0:
            break
    assert all_data == scan_data  # Verify we got all data
    # Note: The number of iterations might vary, we just verify the data is complete

    # Test scan on empty hash
    cursor, pairs = await client.hscan("empty_hash", 0)
    assert cursor == 0
    assert pairs == {}

    # Test scan on non-existent hash
    cursor, pairs = await client.hscan("nonexistent_hash", 0)
    assert cursor == 0
    assert pairs == {}

    # Test scan with special characters in pattern
    await client.hset("scan_hash", "special:field:1", "value1")
    await client.hset("scan_hash", "special:field:2", "value2")
    cursor = 0
    special_fields = {}
    while True:
        cursor, pairs = await client.hscan("scan_hash", cursor, match="special:*")
        special_fields.update(pairs)
        if cursor == 0:
            break
    assert len(special_fields) == 2
    assert "special:field:1" in special_fields
    assert "special:field:2" in special_fields

    # Clean up scan test data
    await client.delete("scan_hash")

    # Test hrandfield operations
    # Prepare test data
    random_fields = {
        f"field_{i}": f"value_{i}"
        for i in range(10)  # Create 10 fields for testing random selection
    }
    await client.hmset("random_hash", random_fields)

    # Test single random field
    field = await client.hrandfield("random_hash", 1)
    assert isinstance(field, list)
    assert len(field) == 1
    assert field[0] in random_fields.keys()

    # Test multiple random fields
    fields = await client.hrandfield("random_hash", 5)
    assert isinstance(fields, list)
    assert len(fields) <= 5  # Might return fewer fields than requested
    assert all(f in random_fields.keys() for f in fields)

    # Test with count larger than hash size
    fields = await client.hrandfield("random_hash", 20)
    assert isinstance(fields, list)
    assert len(fields) <= len(random_fields)  # Cannot return more fields than exist
    assert all(f in random_fields.keys() for f in fields)

    # Test with negative count (should allow duplicates)
    fields = await client.hrandfield("random_hash", -5)
    assert isinstance(fields, list)
    assert len(fields) == 5  # Negative count always returns exactly that many fields
    assert all(f in random_fields.keys() for f in fields)

    # Test with withvalues=True
    result = await client.hrandfield("random_hash", 3, withvalues=True)
    assert isinstance(result, list)
    # Redis returns a flat list alternating between fields and values
    fields_with_values = list(zip(result[::2], result[1::2]))
    assert len(fields_with_values) > 0
    for field, value in fields_with_values:
        assert field in random_fields
        assert value == random_fields[field]

    # Test on empty hash
    assert await client.hrandfield("empty_hash", 1) == []
    assert await client.hrandfield("empty_hash", 1, withvalues=True) == []

    # Test on non-existent hash
    assert await client.hrandfield("nonexistent_hash", 1) == []
    assert await client.hrandfield("nonexistent_hash", 1, withvalues=True) == []

    # Clean up random test data
    await client.delete("random_hash")

    # Test hstrlen operations
    # Prepare test data
    string_lengths = {
        "empty": "",  # Length 0
        "ascii": "hello",  # Length 5 (5 bytes)
        "unicode": "你好世界",  # Length 12 (each Chinese character takes 3 bytes in UTF-8)
        "mixed": "hello世界",  # Length 11 (5 ASCII + 6 bytes for Chinese)
        "spaces": "   ",  # Length 3
        "special": "!@#$%^&*()",  # Length 10
    }
    await client.hmset("strlen_hash", string_lengths)

    # Test string lengths
    assert await client.hstrlen("strlen_hash", "empty") == 0
    assert await client.hstrlen("strlen_hash", "ascii") == 5
    assert (
        await client.hstrlen("strlen_hash", "unicode") == 12
    )  # 4 characters * 3 bytes
    assert await client.hstrlen("strlen_hash", "mixed") == 11  # 5 + (2 * 3) bytes
    assert await client.hstrlen("strlen_hash", "spaces") == 3
    assert await client.hstrlen("strlen_hash", "special") == 10

    # Test non-existent field
    assert await client.hstrlen("strlen_hash", "nonexistent") == 0

    # Test non-existent hash
    assert await client.hstrlen("strlen_hash", "field") == 0

    # Test with special characters in field names
    await client.hset("strlen_hash", "field:with:colon", "test")
    assert await client.hstrlen("strlen_hash", "field:with:colon") == 4

    await client.hset("strlen_hash", "field_with_unicode_中文", "test测试")
    assert (
        await client.hstrlen("strlen_hash", "field_with_unicode_中文") == 10
    )  # 4 + (2 * 3) bytes

    # Clean up test data
    await client.delete("hash_key", "empty_hash", "large_hash")


async def test_list_basic_operations(client: NamespaceClient) -> None:
    """Test basic list operations"""
    # Clean up any existing test data
    await client.delete("list_key")

    # Test adding elements from left
    await client.lpush("list_key", "item1", "item2")
    assert await client.lrange("list_key", 0, -1) == ["item2", "item1"]
    assert await t.cast(
        t.Awaitable[list], client.client.lrange(client.make_key("list_key"), 0, -1)
    ) == [
        "item2",
        "item1",
    ]
    # Test adding elements from right
    await client.rpush("list_key", "item3")
    assert await client.lrange("list_key", 0, -1) == ["item2", "item1", "item3"]
    assert await t.cast(
        t.Awaitable[list], client.client.lrange(client.make_key("list_key"), 0, -1)
    ) == [
        "item2",
        "item1",
        "item3",
    ]
    # Test popping elements from left
    assert await client.lpop("list_key") == "item2"
    assert await client.lrange("list_key", 0, -1) == ["item1", "item3"]
    assert await t.cast(
        t.Awaitable[list], client.client.lrange(client.make_key("list_key"), 0, -1)
    ) == [
        "item1",
        "item3",
    ]
    # Test popping elements from right
    assert await client.rpop("list_key") == "item3"
    assert await client.lrange("list_key", 0, -1) == ["item1"]
    assert await t.cast(
        t.Awaitable[list], client.client.lrange(client.make_key("list_key"), 0, -1)
    ) == ["item1"]

    # Clean up test data
    await client.delete("list_key")


async def test_list_special_cases(client: NamespaceClient) -> None:
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


async def test_set_basic_operations(client: NamespaceClient) -> None:
    """Test basic set operations"""
    # Clean up any existing test data
    await client.delete("set_key")

    # Test adding members
    await client.sadd("set_key", "member1", "member2", "member3")
    assert await client.smembers("set_key") == {"member1", "member2", "member3"}
    assert await t.cast(
        t.Awaitable[t.Set[t.Any]], client.client.smembers(client.make_key("set_key"))
    ) == {
        "member1",
        "member2",
        "member3",
    }
    # Test checking member existence
    assert await client.sismember("set_key", "member1")
    assert await t.cast(
        t.Awaitable[t.Literal[0, 1]],
        client.client.sismember(client.make_key("set_key"), "member1"),
    )
    assert not await client.sismember("set_key", "nonexistent")

    # Test getting set size
    assert await client.scard("set_key") == 3
    assert (
        await t.cast(t.Awaitable[int], client.client.scard(client.make_key("set_key")))
        == 3
    )
    # Test removing member
    await client.srem("set_key", "member1")
    assert not await client.sismember("set_key", "member1")
    assert await client.scard("set_key") == 2
    assert (
        await t.cast(t.Awaitable[int], client.client.scard(client.make_key("set_key")))
        == 2
    )

    # Clean up test data
    await client.delete("set_key")


async def test_set_special_cases(client: NamespaceClient) -> None:
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

    # Test spop operations
    # Prepare test data
    test_members = {f"member_{i}" for i in range(10)}
    await client.sadd("pop_set", *test_members)

    # Test single pop
    popped = await client.spop("pop_set", 1)
    assert isinstance(popped, list)
    assert len(popped) == 1
    assert popped[0] in test_members
    remaining = await client.smembers("pop_set")
    assert len(remaining) == 9
    assert not (set(popped) & remaining)  # No intersection between popped and remaining

    # Test multiple pop
    popped_multi = await client.spop("pop_set", 3)
    assert isinstance(popped_multi, list)
    assert len(popped_multi) == 3
    assert set(popped_multi).issubset(test_members)
    remaining = await client.smembers("pop_set")
    assert len(remaining) == 6
    assert not (set(popped_multi) & remaining)  # No intersection

    # Test pop more than remaining
    remaining_count = len(remaining)
    popped_all = await client.spop("pop_set", 10)
    assert isinstance(popped_all, list)
    assert len(popped_all) == remaining_count  # Should only pop remaining items
    assert set(popped_all).issubset(test_members)
    assert await client.scard("pop_set") == 0  # Set should be empty

    # Test pop from empty set
    assert await client.spop("pop_set", 1) == []

    # Test pop from non-existent set
    assert await client.spop("nonexistent_set", 1) == []

    # Test with special characters
    special_members = {"test:with:colon", "test with space", "test_with_unicode_中文"}
    await client.sadd("special_pop_set", *special_members)
    popped = await client.spop("special_pop_set", 2)
    assert isinstance(popped, list)
    assert len(popped) == 2
    assert set(popped).issubset(special_members)

    # Clean up test data
    await client.delete("pop_set", "special_pop_set")

    # Test smismember operations
    # Prepare test data
    test_set = {
        "member1",
        "member2",
        "member3",
        "member:with:colon",
        "member_with_中文",
    }
    await client.sadd("mismember_set", *test_set)

    # Test basic multiple membership check
    result = await client.smismember(
        "mismember_set", ["member1", "member2", "nonexistent"]
    )
    assert result == [1, 1, 0]  # [True, True, False]

    # Test with special characters
    result = await client.smismember(
        "mismember_set", ["member:with:colon", "member_with_中文", "nonexistent:member"]
    )
    assert result == [1, 1, 0]  # Both special character members exist

    # Test with non-existent set
    result = await client.smismember("nonexistent_set", ["member1", "member2"])
    assert result == [0, 0]  # No members exist in non-existent set

    # Test with duplicate members in query
    result = await client.smismember("mismember_set", ["member1", "member1", "member2"])
    assert result == [1, 1, 1]  # Duplicates are allowed in query

    # Test with empty string member
    await client.sadd("mismember_set", "")
    result = await client.smismember("mismember_set", ["", "member1"])
    assert result == [1, 1]  # Empty string is a valid member

    # Clean up test data
    await client.delete("mismember_set")

    # Test srandmember operations
    # Prepare test data
    test_members = {f"member_{i}" for i in range(10)}
    await client.sadd("random_set", *test_members)

    # Test single random member
    member = await client.srandmember("random_set", 1)
    assert isinstance(member, list)
    assert len(member) == 1
    assert member[0] in test_members

    # Test multiple random members (without duplicates)
    members = await client.srandmember("random_set", 5)
    assert isinstance(members, list)
    assert len(members) <= 5  # Might return fewer members than requested
    assert len(set(members)) == len(members)  # No duplicates
    assert set(members).issubset(test_members)

    # Test with count larger than set size
    members = await client.srandmember("random_set", 20)
    assert isinstance(members, list)
    assert len(members) <= len(test_members)  # Cannot return more members than exist
    assert len(set(members)) == len(members)  # No duplicates
    assert set(members).issubset(test_members)

    # Test with negative count (should allow duplicates)
    members = await client.srandmember("random_set", -5)
    assert isinstance(members, list)
    assert len(members) == 5  # Negative count always returns exactly that many members
    assert all(m in test_members for m in members)  # All members should be valid
    # Note: May or may not have duplicates, can't reliably test for them

    # Test on empty set
    assert await client.srandmember("empty_set", 1) == []
    assert await client.srandmember("empty_set", 5) == []
    assert await client.srandmember("empty_set", -5) == []

    # Test on non-existent set
    assert await client.srandmember("nonexistent_set", 1) == []
    assert await client.srandmember("nonexistent_set", 5) == []
    assert await client.srandmember("nonexistent_set", -5) == []

    # Test with special characters
    special_members = {"test:with:colon", "test with space", "test_with_unicode_中文"}
    await client.sadd("special_random_set", *special_members)
    members = await client.srandmember("special_random_set", len(special_members))
    assert isinstance(members, list)
    assert set(members) == special_members

    # Clean up test data
    await client.delete("random_set", "special_random_set")

    # Test sscan operations
    # Prepare test data with a larger dataset for scanning
    scan_members = {
        f"scan_member_{i}"
        for i in range(100)  # Create 100 members for testing scan
    }
    await client.sadd("scan_set", *scan_members)

    # Test basic scan
    cursor = 0
    all_members = set()
    while True:
        cursor, members = await client.sscan("scan_set", cursor)
        all_members.update(members)
        if cursor == 0:  # Iteration complete
            break
    assert all_members == scan_members

    # Test scan with match pattern
    cursor = 0
    matched_members = set()
    while True:
        cursor, members = await client.sscan("scan_set", cursor, match="scan_member_1*")
        matched_members.update(members)
        if cursor == 0:
            break
    # Should match scan_member_1, scan_member_10-19, scan_member_100
    assert all(m.startswith("scan_member_1") for m in matched_members)

    # Test scan with count
    cursor = 0
    first_batch = set()
    cursor, members = await client.sscan("scan_set", cursor, count=10)
    first_batch.update(members)
    assert len(first_batch) > 0  # Should return some members
    # Note: Even with small count, Redis might return all items in one iteration
    # So we only verify that we got some data, not the cursor value

    # Test scan with count in multiple iterations
    all_data = set()
    iterations = 0
    cursor = 0
    while True:
        cursor, members = await client.sscan("scan_set", cursor, count=10)
        iterations += 1
        all_data.update(members)
        if cursor == 0:
            break
    assert all_data == scan_members  # Verify we got all data
    # Note: The number of iterations might vary, we just verify the data is complete

    # Test scan on empty set
    cursor, members = await client.sscan("empty_set", 0)
    assert cursor == 0
    assert members == []

    # Test scan on non-existent set
    cursor, members = await client.sscan("nonexistent_set", 0)
    assert cursor == 0
    assert members == []

    # Test scan with special characters in pattern
    special_scan_members = {
        "test:with:colon",
        "test with space",
        "test_with_unicode_中文",
    }
    await client.sadd("special_scan_set", *special_scan_members)

    cursor = 0
    special_results = set()
    while True:
        cursor, members = await client.sscan("special_scan_set", cursor, match="test:*")
        special_results.update(members)
        if cursor == 0:
            break
    assert len(special_results) == 1
    assert "test:with:colon" in special_results

    # Clean up scan test data
    await client.delete("scan_set", "special_scan_set")

    # Test sinterstore operations
    # Prepare test data
    await client.sadd("set1", "a", "b", "c", "d")
    await client.sadd("set2", "b", "c", "e")
    await client.sadd("set3", "b", "c", "f")
    await client.sadd("set_empty", "")

    # Test basic intersection store
    count = await client.sinterstore("dest_set", ["set1", "set2"])
    assert count == 2  # Number of elements in intersection
    result = await client.smembers("dest_set")
    assert result == {"b", "c"}

    # Test intersection store with multiple sets
    count = await client.sinterstore("dest_set", ["set1", "set2", "set3"])
    assert count == 2  # Only "b" and "c" are common to all sets
    result = await client.smembers("dest_set")
    assert result == {"b", "c"}

    # Test with empty set
    count = await client.sinterstore("dest_set", ["set1", "set_empty"])
    assert count == 0  # Empty intersection
    result = await client.smembers("dest_set")
    assert result == set()

    # Test with non-existent set
    count = await client.sinterstore("dest_set", ["set1", "nonexistent_set"])
    assert count == 0  # Empty intersection
    result = await client.smembers("dest_set")
    assert result == set()

    # Test with destination same as source
    await client.sadd("source_set", "x", "y", "z")
    count = await client.sinterstore("source_set", ["source_set", "set1"])
    assert count == 0  # No common elements
    result = await client.smembers("source_set")
    assert result == set()  # Source set is overwritten

    # Test with special characters
    await client.sadd("special_set1", "test:1", "test:2", "test:3")
    await client.sadd("special_set2", "test:2", "test:3", "test:4")
    count = await client.sinterstore("dest_set", ["special_set1", "special_set2"])
    assert count == 2
    result = await client.smembers("dest_set")
    assert result == {"test:2", "test:3"}

    # Clean up test data
    await client.delete(
        "set1",
        "set2",
        "set3",
        "set_empty",
        "dest_set",
        "source_set",
        "special_set1",
        "special_set2",
    )

    # Test sunionstore operations
    # Prepare test data
    await client.sadd("union_set1", "a", "b", "c", "d")
    await client.sadd("union_set2", "c", "d", "e")
    await client.sadd("union_set3", "d", "e", "f")
    await client.sadd("union_empty", "")

    # Test basic union store
    count = await client.sunionstore("dest_union", ["union_set1", "union_set2"])
    assert count == 5  # Number of elements in union
    result = await client.smembers("dest_union")
    assert result == {"a", "b", "c", "d", "e"}

    # Test union store with multiple sets
    count = await client.sunionstore(
        "dest_union", ["union_set1", "union_set2", "union_set3"]
    )
    assert count == 6  # Union of all unique elements
    result = await client.smembers("dest_union")
    assert result == {"a", "b", "c", "d", "e", "f"}

    # Test with empty set
    count = await client.sunionstore("dest_union", ["union_set1", "union_empty"])
    assert count == 5  # Empty set doesn't affect union
    result = await client.smembers("dest_union")
    assert result == {"a", "b", "c", "d", ""}

    # Test with non-existent set
    count = await client.sunionstore("dest_union", ["union_set1", "nonexistent_set"])
    assert count == 4  # Non-existent set treated as empty
    result = await client.smembers("dest_union")
    assert result == {"a", "b", "c", "d"}

    # Test with destination same as source
    await client.sadd("source_union", "x", "y", "z")
    count = await client.sunionstore("source_union", ["source_union", "union_set1"])
    assert count == 7  # Combined unique elements
    result = await client.smembers("source_union")
    assert result == {"a", "b", "c", "d", "x", "y", "z"}

    # Test with special characters
    await client.sadd("special_union1", "test:1", "test:2", "test:3")
    await client.sadd("special_union2", "test:3", "test:4", "test with space")
    count = await client.sunionstore("dest_union", ["special_union1", "special_union2"])
    assert count == 5
    result = await client.smembers("dest_union")
    assert result == {"test:1", "test:2", "test:3", "test:4", "test with space"}

    # Test union of a set with itself
    await client.sadd("self_union", "a", "b", "c")
    count = await client.sunionstore("dest_union", ["self_union", "self_union"])
    assert count == 3  # Should not duplicate elements
    result = await client.smembers("dest_union")
    assert result == {"a", "b", "c"}

    # Clean up test data
    await client.delete(
        "union_set1",
        "union_set2",
        "union_set3",
        "union_empty",
        "dest_union",
        "source_union",
        "special_union1",
        "special_union2",
        "self_union",
    )

    # Test sdiffstore operations
    # Prepare test data
    await client.sadd("diff_set1", "a", "b", "c", "d")
    await client.sadd("diff_set2", "c", "d", "e")
    await client.sadd("diff_set3", "d", "e", "f")
    await client.sadd("diff_empty", "")

    # Test basic difference store
    count = await client.sdiffstore("dest_diff", ["diff_set1", "diff_set2"])
    assert count == 2  # Number of elements in difference
    result = await client.smembers("dest_diff")
    assert result == {"a", "b"}  # Elements in set1 but not in set2

    # Test difference store with multiple sets
    count = await client.sdiffstore(
        "dest_diff", ["diff_set1", "diff_set2", "diff_set3"]
    )
    assert count == 2  # Only "a" and "b" remain after subtracting both sets
    result = await client.smembers("dest_diff")
    assert result == {"a", "b"}

    # Test with empty set
    count = await client.sdiffstore("dest_diff", ["diff_set1", "diff_empty"])
    assert count == 4  # Empty set doesn't affect difference
    result = await client.smembers("dest_diff")
    assert result == {"a", "b", "c", "d"}

    # Test with non-existent set
    count = await client.sdiffstore("dest_diff", ["diff_set1", "nonexistent_set"])
    assert count == 4  # Non-existent set treated as empty
    result = await client.smembers("dest_diff")
    assert result == {"a", "b", "c", "d"}

    # Test with destination same as source
    await client.sadd("source_diff", "x", "y", "z")
    count = await client.sdiffstore("source_diff", ["source_diff", "diff_set1"])
    assert count == 3  # Elements unique to source_diff
    result = await client.smembers("source_diff")
    assert result == {"x", "y", "z"}

    # Test with special characters
    await client.sadd("special_diff1", "test:1", "test:2", "test:3")
    await client.sadd("special_diff2", "test:2", "test:3", "test:4")
    count = await client.sdiffstore("dest_diff", ["special_diff1", "special_diff2"])
    assert count == 1
    result = await client.smembers("dest_diff")
    assert result == {"test:1"}  # Only element in diff1 but not in diff2

    # Test difference with itself (should be empty)
    await client.sadd("self_diff", "a", "b", "c")
    count = await client.sdiffstore("dest_diff", ["self_diff", "self_diff"])
    assert count == 0  # No difference between a set and itself
    result = await client.smembers("dest_diff")
    assert result == set()

    # Clean up test data
    await client.delete(
        "diff_set1",
        "diff_set2",
        "diff_set3",
        "diff_empty",
        "dest_diff",
        "source_diff",
        "special_diff1",
        "special_diff2",
        "self_diff",
    )

    # Test smove operations
    # Prepare test data
    await client.sadd("source_set", "a", "b", "c", "d")
    await client.sadd("dest_set", "c", "d", "e")

    # Test basic move
    assert await client.smove("source_set", "dest_set", "a") is True
    assert not await client.sismember(
        "source_set", "a"
    )  # Should be removed from source
    assert await client.sismember("dest_set", "a")  # Should be added to destination

    # Test moving already existing member
    assert await client.smove("source_set", "dest_set", "c") is True
    source_members = await client.smembers("source_set")
    dest_members = await client.smembers("dest_set")
    assert "c" not in source_members
    assert "c" in dest_members

    # Test moving non-existent member
    assert await client.smove("source_set", "dest_set", "nonexistent") is False

    # Test moving to non-existent destination set (should create it)
    assert await client.smove("source_set", "new_dest_set", "b") is True
    assert await client.sismember("new_dest_set", "b")
    assert not await client.sismember("source_set", "b")

    # Test with empty string member
    await client.sadd("source_set", "")
    assert await client.smove("source_set", "dest_set", "") is True
    assert not await client.sismember("source_set", "")
    assert await client.sismember("dest_set", "")

    # Test with special characters
    await client.sadd("special_source", "test:1", "test:2", "test with space")
    assert await client.smove("special_source", "special_dest", "test:1") is True
    assert (
        await client.smove("special_source", "special_dest", "test with space") is True
    )
    assert not await client.sismember("special_source", "test:1")
    assert not await client.sismember("special_source", "test with space")
    assert await client.sismember("special_dest", "test:1")
    assert await client.sismember("special_dest", "test with space")

    # Test moving from empty set
    await client.delete("empty_source")
    assert await client.smove("empty_source", "dest_set", "any") is False

    # Test moving from non-existent set
    assert await client.smove("nonexistent_set", "dest_set", "any") is False

    # Clean up test data
    await client.delete(
        "source_set",
        "dest_set",
        "new_dest_set",
        "special_source",
        "special_dest",
        "empty_source",
    )


async def test_zset_basic_operations(client: NamespaceClient) -> None:
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


async def test_zset_special_cases(client: NamespaceClient) -> None:
    """Test special cases for zset operations"""
    # Test zrem operations
    # Prepare test data
    test_members = {
        "member1": 1.0,
        "member2": 2.0,
        "member3": 3.0,
        "member:with:colon": 4.0,
        "member with space": 5.0,
        "member_with_unicode_中文": 6.0,
        "": 7.0,  # Empty string member
    }
    await client.zadd("zrem_set", test_members)

    # Test removing single member
    assert await client.zrem("zrem_set", "member1") == 1
    assert await client.zscore("zrem_set", "member1") is None

    # Test removing multiple members
    assert await client.zrem("zrem_set", "member2", "member3") == 2
    assert await client.zscore("zrem_set", "member2") is None
    assert await client.zscore("zrem_set", "member3") is None

    # Test removing non-existent members
    assert await client.zrem("zrem_set", "nonexistent1", "nonexistent2") == 0

    # Test removing members with special characters
    assert await client.zrem("zrem_set", "member:with:colon", "member with space") == 2
    assert await client.zscore("zrem_set", "member:with:colon") is None
    assert await client.zscore("zrem_set", "member with space") is None

    # Test removing member with Unicode characters
    assert await client.zrem("zrem_set", "member_with_unicode_中文") == 1
    assert await client.zscore("zrem_set", "member_with_unicode_中文") is None

    # Test removing empty string member
    assert await client.zrem("zrem_set", "") == 1
    assert await client.zscore("zrem_set", "") is None

    # Test removing from empty sorted set
    assert await client.zrem("zrem_set", "any_member") == 0

    # Test removing from non-existent sorted set
    assert await client.zrem("nonexistent_zset", "any_member") == 0

    # Test removing with mixed existing and non-existing members
    await client.zadd("zrem_set", {"a": 1.0, "b": 2.0, "c": 3.0})
    assert await client.zrem("zrem_set", "a", "nonexistent", "b") == 2
    assert await client.zcard("zrem_set") == 1
    assert await client.zscore("zrem_set", "c") == 3.0

    # Clean up test data
    await client.delete("zrem_set")

    # Test zincrby operations
    # Prepare test data
    test_scores = {
        "member1": 1.0,
        "member2": 2.5,
        "member:with:colon": 3.14,
        "member with space": -1.5,
        "member_with_unicode_中文": 0.0,
    }
    await client.zadd("incr_set", test_scores)

    # Test basic increment
    assert await client.zincrby("incr_set", 2.0, "member1") == 3.0
    assert await client.zscore("incr_set", "member1") == 3.0

    # Test increment with negative value (decrement)
    assert await client.zincrby("incr_set", -1.5, "member2") == 1.0
    assert await client.zscore("incr_set", "member2") == 1.0

    # Test increment with float value
    assert await client.zincrby("incr_set", 0.5, "member:with:colon") == 3.64
    assert await client.zscore("incr_set", "member:with:colon") == 3.64

    # Test increment with negative score
    assert await client.zincrby("incr_set", -2.5, "member with space") == -4.0
    assert await client.zscore("incr_set", "member with space") == -4.0

    # Test increment with zero score
    assert await client.zincrby("incr_set", 1.5, "member_with_unicode_中文") == 1.5
    assert await client.zscore("incr_set", "member_with_unicode_中文") == 1.5

    # Test increment non-existent member (should create it)
    assert await client.zincrby("incr_set", 3.0, "new_member") == 3.0
    assert await client.zscore("incr_set", "new_member") == 3.0

    # Test increment in non-existent sorted set (should create it)
    assert await client.zincrby("new_incr_set", 2.5, "first_member") == 2.5
    assert await client.zscore("new_incr_set", "first_member") == 2.5

    # Test increment with very small/large numbers
    assert await client.zincrby("incr_set", 1e-10, "small_incr") == 1e-10
    assert await client.zincrby("incr_set", 1e10, "large_incr") == 1e10

    # Test multiple increments on same member
    member = "multi_incr"
    await client.zadd("incr_set", {member: 1.0})
    assert await client.zincrby("incr_set", 1.5, member) == 2.5
    assert await client.zincrby("incr_set", -1.0, member) == 1.5
    assert await client.zincrby("incr_set", 0.5, member) == 2.0
    assert await client.zscore("incr_set", member) == 2.0

    # Clean up test data
    await client.delete("incr_set", "new_incr_set")

    # Test zrangebyscore operations
    # Prepare test data
    score_members = {
        "member1": -1.5,
        "member2": 0.0,
        "member3": 2.5,
        "member4": 3.14,
        "member5": 5.0,
        "member:with:colon": 6.0,
        "member with space": 7.5,
        "member_with_unicode_中文": 10.0,
    }
    await client.zadd("score_set", score_members)

    # Test basic range query
    result = await client.zrangebyscore("score_set", 2.5, 6.0)
    assert set(result) == {"member3", "member4", "member5", "member:with:colon"}

    # Test with withscores=True
    result = await client.zrangebyscore("score_set", 2.5, 6.0, withscores=True)
    assert dict(result) == {
        "member3": 2.5,
        "member4": 3.14,
        "member5": 5.0,
        "member:with:colon": 6.0,
    }

    # Test with negative scores
    result = await client.zrangebyscore("score_set", -2, 0)
    assert set(result) == {"member1", "member2"}

    # Test with infinity bounds
    result = await client.zrangebyscore("score_set", "-inf", "+inf")
    assert len(result) == len(score_members)
    assert result[0] == "member1"  # Lowest score
    assert result[-1] == "member_with_unicode_中文"  # Highest score

    # Test with exclusive bounds
    result = await client.zrangebyscore("score_set", "(2.5", "(6.0")
    assert set(result) == {
        "member4",
        "member5",
    }  # Only scores strictly between 2.5 and 6.0

    # Test with mixed bounds
    result = await client.zrangebyscore("score_set", "2.5", "(6.0")
    assert set(result) == {
        "member3",
        "member4",
        "member5",
    }  # Including 2.5 but excluding 6.0

    # Test with offset and count
    result = await client.zrangebyscore("score_set", "-inf", "+inf", offset=2, count=3)
    assert len(result) == 3
    assert set(result).issubset(set(score_members.keys()))

    # Test with empty range
    result = await client.zrangebyscore("score_set", 100, 200)
    assert result == []

    # Test with min > max
    result = await client.zrangebyscore("score_set", 5, 2)
    assert result == []

    # Test on empty sorted set
    result = await client.zrangebyscore("empty_set", 0, 1)
    assert result == []

    # Test on non-existent sorted set
    result = await client.zrangebyscore("nonexistent_set", 0, 1)
    assert result == []

    # Clean up test data
    await client.delete("score_set")

    # Test zrevrangebyscore operations
    # Prepare test data
    score_members = {
        "member1": -1.5,
        "member2": 0.0,
        "member3": 2.5,
        "member4": 3.14,
        "member5": 5.0,
        "member:with:colon": 6.0,
        "member with space": 7.5,
        "member_with_unicode_中文": 10.0,
    }
    await client.zadd("revscore_set", score_members)

    # Test basic reverse range query
    result = await client.zrevrangebyscore("revscore_set", 6.0, 2.5)
    assert set(result) == {"member3", "member4", "member5", "member:with:colon"}

    # Test with withscores=True
    result = await client.zrevrangebyscore("revscore_set", 6.0, 2.5, withscores=True)
    assert dict(result) == {
        "member:with:colon": 6.0,
        "member5": 5.0,
        "member4": 3.14,
        "member3": 2.5,
    }

    # Test with negative scores
    result = await client.zrevrangebyscore("revscore_set", 0, -2)
    assert set(result) == {"member1", "member2"}

    # Test with infinity bounds
    result = await client.zrevrangebyscore("revscore_set", "+inf", "-inf")
    assert len(result) == len(score_members)
    assert result[0] == "member_with_unicode_中文"  # Highest score
    assert result[-1] == "member1"  # Lowest score

    # Test with exclusive bounds
    result = await client.zrevrangebyscore("revscore_set", "(6.0", "(2.5")
    assert set(result) == {
        "member4",
        "member5",
    }  # Only scores strictly between 2.5 and 6.0

    # Test with mixed bounds
    result = await client.zrevrangebyscore("revscore_set", "6.0", "(2.5")
    assert set(result) == {
        "member4",
        "member5",
        "member:with:colon",
    }  # Including 6.0 but excluding 2.5

    # Test with offset and count
    result = await client.zrevrangebyscore(
        "revscore_set", "+inf", "-inf", offset=2, count=3
    )
    assert len(result) == 3
    assert set(result).issubset(set(score_members.keys()))

    # Test with empty range
    result = await client.zrevrangebyscore("revscore_set", 2, 5)
    assert result == []

    # Test with min > max
    result = await client.zrevrangebyscore("revscore_set", 2, 5)
    assert result == []

    # Test on empty sorted set
    result = await client.zrevrangebyscore("empty_set", 1, 0)
    assert result == []

    # Test on non-existent sorted set
    result = await client.zrevrangebyscore("nonexistent_set", 1, 0)
    assert result == []

    # Clean up test data
    await client.delete("revscore_set")

    # Test zremrangebyrank operations
    # Prepare test data
    rank_members = {
        "member1": 1.0,
        "member2": 2.0,
        "member3": 3.0,
        "member4": 4.0,
        "member5": 5.0,
        "member:with:colon": 6.0,
        "member with space": 7.0,
        "member_with_unicode_中文": 8.0,
    }
    await client.zadd("rank_set", rank_members)

    # Test removing members by rank range
    # Remove first two members (lowest scores)
    assert await client.zremrangebyrank("rank_set", 0, 1) == 2
    result = await client.zrange("rank_set", 0, -1)
    assert "member1" not in result and "member2" not in result

    # Test removing members from middle
    assert await client.zremrangebyrank("rank_set", 2, 3) == 2
    result = await client.zrange("rank_set", 0, -1)
    assert "member5" not in result and "member:with:colon" not in result

    # Test removing last member
    assert await client.zremrangebyrank("rank_set", -1, -1) == 1
    result = await client.zrange("rank_set", 0, -1)
    assert "member_with_unicode_中文" not in result

    # Test removing with negative indices
    await client.delete("rank_set")
    await client.zadd("rank_set", rank_members)
    assert await client.zremrangebyrank("rank_set", -3, -1) == 3
    result = await client.zrange("rank_set", 0, -1)
    assert all(
        m not in result
        for m in ["member with space", "member:with:colon", "member_with_unicode_中文"]
    )

    # Test removing all members
    await client.delete("rank_set")
    await client.zadd("rank_set", rank_members)
    assert await client.zremrangebyrank("rank_set", 0, -1) == len(rank_members)
    assert await client.zcard("rank_set") == 0

    # Test with invalid range (start > end)
    assert await client.zremrangebyrank("rank_set", 5, 3) == 0

    # Test with out of range indices
    await client.zadd("rank_set", {"a": 1, "b": 2})
    assert await client.zremrangebyrank("rank_set", 5, 10) == 0
    assert await client.zremrangebyrank("rank_set", -5, -3) == 0

    # Test with empty sorted set
    assert await client.zremrangebyrank("empty_set", 0, 1) == 0

    # Test with non-existent sorted set
    assert await client.zremrangebyrank("nonexistent_set", 0, 1) == 0

    # Clean up test data
    await client.delete("rank_set")

    # Test zremrangebyscore operations
    # Prepare test data
    score_members = {
        "member1": -1.5,
        "member2": 0.0,
        "member3": 2.5,
        "member4": 3.14,
        "member5": 5.0,
        "member:with:colon": 6.0,
        "member with space": 7.5,
        "member_with_unicode_中文": 10.0,
    }
    await client.zadd("score_range_set", score_members)

    # Test removing members by score range
    # Remove members with scores between -2 and 0 (inclusive)
    assert await client.zremrangebyscore("score_range_set", -2, 0) == 2
    result = await client.zrange("score_range_set", 0, -1)
    assert "member1" not in result and "member2" not in result

    # Test removing members with exclusive bounds
    assert await client.zremrangebyscore("score_range_set", "(2.5", "(6.0") == 2
    result = await client.zrange("score_range_set", 0, -1)
    assert "member4" not in result and "member5" not in result

    # Test removing members with mixed bounds
    await client.delete("score_range_set")
    await client.zadd("score_range_set", score_members)
    assert (
        await client.zremrangebyscore("score_range_set", "5.0", "(10.0") == 3
    )  # Changed from 2 to 3
    result = await client.zrange("score_range_set", 0, -1)
    # Should remove member5 (5.0), member:with:colon (6.0), and member with space (7.5)
    assert "member5" not in result
    assert "member:with:colon" not in result
    assert "member with space" not in result

    # Test removing with infinity bounds
    await client.delete("score_range_set")
    await client.zadd("score_range_set", score_members)
    assert await client.zremrangebyscore("score_range_set", "-inf", "+inf") == len(
        score_members
    )
    assert await client.zcard("score_range_set") == 0

    # Test with invalid range (min > max)
    assert await client.zremrangebyscore("score_range_set", 5, 2) == 0

    # Test with empty range
    await client.zadd("score_range_set", {"a": 1, "b": 2})
    assert await client.zremrangebyscore("score_range_set", 100, 200) == 0

    # Test with empty sorted set
    assert await client.zremrangebyscore("empty_set", 0, 1) == 0

    # Test with non-existent sorted set
    assert await client.zremrangebyscore("nonexistent_set", 0, 1) == 0

    # Test removing specific score
    await client.zadd("score_range_set", {"exact1": 3.0, "exact2": 3.0, "other": 4.0})
    assert await client.zremrangebyscore("score_range_set", 3.0, 3.0) == 2
    result = await client.zrange("score_range_set", 0, -1)
    assert "exact1" not in result and "exact2" not in result
    assert "other" in result

    # Clean up test data
    await client.delete("score_range_set")

    # Test zrandmember operations
    # Prepare test data
    rand_members = {
        "member1": 1.0,
        "member2": 2.0,
        "member3": 3.0,
        "member4": 4.0,
        "member5": 5.0,
        "member:with:colon": 6.0,
        "member with space": 7.0,
        "member_with_unicode_中文": 8.0,
    }
    await client.zadd("rand_set", rand_members)

    # Test getting single random member
    result = await client.zrandmember("rand_set")
    assert isinstance(result, str)
    assert result in rand_members

    # Test getting single random member with scores
    result = await client.zrandmember("rand_set", count=1, withscores=True)
    assert isinstance(result, list)
    assert len(result) == 2  # Returns [member, score] for single member
    assert result[0] in rand_members  # member
    assert float(result[1]) == rand_members[result[0]]  # score

    # Test getting multiple random members
    result = await client.zrandmember("rand_set", count=3)
    assert isinstance(result, list)
    assert len(result) == 3
    assert all(member in rand_members for member in result)

    # Test getting multiple random members with scores
    result = await client.zrandmember("rand_set", count=3, withscores=True)
    assert isinstance(result, list)
    assert len(result) % 2 == 0  # Should be even number (member, score pairs)
    members = result[::2]  # Even indices are members
    scores = result[1::2]  # Odd indices are scores
    assert all(member in rand_members for member in members)
    assert all(
        float(score) == rand_members[member] for member, score in zip(members, scores)
    )

    # Test getting more members than exist (with duplicates)
    result = await client.zrandmember("rand_set", count=10)
    assert isinstance(result, list)
    assert len(result) == len(
        rand_members
    )  # Redis returns all members when count > set size
    assert all(member in rand_members for member in result)

    # Test getting more members than exist (with duplicates, negative count)
    result = await client.zrandmember("rand_set", count=-10)
    assert isinstance(result, list)
    assert (
        len(result) == 10
    )  # Negative count returns absolute value number of members with duplicates
    assert all(
        member in rand_members for member in result
    )  # All returned members should be valid
    # Note: We don't check for duplicates because they are expected with negative count

    # Test incr operations
    # Test basic increment
    await client.delete("counter")
    assert await client.incr("counter") == 1  # First increment creates counter
    assert await client.incr("counter") == 2  # Second increment
    assert await client.get("counter") == "2"  # Verify value

    # Test increment on existing numeric value
    await client.set("counter", "5")
    assert await client.incr("counter") == 6
    assert await client.get("counter") == "6"

    # Test increment on large numbers
    await client.set("counter", "9999999999")
    assert await client.incr("counter") == 10000000000
    assert await client.get("counter") == "10000000000"

    # Test increment on negative numbers
    await client.set("counter", "-5")
    assert await client.incr("counter") == -4
    assert await client.get("counter") == "-4"

    # Test increment on non-numeric value
    await client.set("counter", "abc")
    with pytest.raises(ResponseError):
        await client.incr("counter")

    # Test increment on non-existent key
    await client.delete("nonexistent_counter")
    assert await client.incr("nonexistent_counter") == 1
    assert await client.get("nonexistent_counter") == "1"

    # Test increment on empty string
    await client.set("counter", "")
    with pytest.raises(ResponseError):
        await client.incr("counter")

    # Test increment on float value
    await client.set("counter", "1.5")
    with pytest.raises(ResponseError):
        await client.incr("counter")

    # Clean up test data
    await client.delete("counter", "nonexistent_counter")

    # Test decr operations
    # Test basic decrement
    await client.delete("counter")
    assert await client.decr("counter") == -1  # First decrement creates counter
    assert await client.decr("counter") == -2  # Second decrement
    assert await client.get("counter") == "-2"  # Verify value

    # Test decrement on existing numeric value
    await client.set("counter", "5")
    assert await client.decr("counter") == 4
    assert await client.get("counter") == "4"

    # Test decrement on large numbers
    await client.set("counter", "10000000000")
    assert await client.decr("counter") == 9999999999
    assert await client.get("counter") == "9999999999"

    # Test decrement on negative numbers
    await client.set("counter", "-5")
    assert await client.decr("counter") == -6
    assert await client.get("counter") == "-6"

    # Test decrement on non-numeric value
    await client.set("counter", "abc")
    with pytest.raises(ResponseError):
        await client.decr("counter")

    # Test decrement on non-existent key
    await client.delete("nonexistent_counter")
    assert await client.decr("nonexistent_counter") == -1
    assert await client.get("nonexistent_counter") == "-1"

    # Test decrement on empty string
    await client.set("counter", "")
    with pytest.raises(ResponseError):
        await client.decr("counter")

    # Test decrement on float value
    await client.set("counter", "1.5")
    with pytest.raises(ResponseError):
        await client.decr("counter")

    # Test decrement to zero and below
    await client.set("counter", "2")
    assert await client.decr("counter") == 1
    assert await client.decr("counter") == 0
    assert await client.decr("counter") == -1

    # Clean up test data
    await client.delete("counter", "nonexistent_counter")

    # Test incrby operations
    # Test basic increment by value
    await client.delete("counter")
    assert await client.incrby("counter", 5) == 5  # First increment creates counter
    assert await client.incrby("counter", 3) == 8  # Second increment
    assert await client.get("counter") == "8"  # Verify value

    # Test increment by negative value (decrement)
    assert await client.incrby("counter", -3) == 5
    assert await client.get("counter") == "5"

    # Test increment by zero
    assert await client.incrby("counter", 0) == 5
    assert await client.get("counter") == "5"

    # Test increment on existing numeric value
    await client.set("counter", "10")
    assert await client.incrby("counter", 5) == 15
    assert await client.get("counter") == "15"

    # Test increment on large numbers
    await client.set("counter", "9999999999")
    assert await client.incrby("counter", 1000) == 10000000999
    assert await client.get("counter") == "10000000999"

    # Test increment on negative numbers
    await client.set("counter", "-5")
    assert await client.incrby("counter", 10) == 5
    assert await client.get("counter") == "5"

    # Test increment by negative value on negative number
    await client.set("counter", "-5")
    assert await client.incrby("counter", -10) == -15
    assert await client.get("counter") == "-15"

    # Test increment on non-numeric value
    await client.set("counter", "abc")
    with pytest.raises(ResponseError):
        await client.incrby("counter", 5)

    # Test increment on non-existent key
    await client.delete("nonexistent_counter")
    assert await client.incrby("nonexistent_counter", 5) == 5
    assert await client.get("nonexistent_counter") == "5"

    # Test increment on empty string
    await client.set("counter", "")
    with pytest.raises(ResponseError):
        await client.incrby("counter", 5)

    # Test increment on float value
    await client.set("counter", "1.5")
    with pytest.raises(ResponseError):
        await client.incrby("counter", 5)

    # Clean up test data
    await client.delete("counter", "nonexistent_counter")

    # Test decrby operations
    # Test basic decrement by value
    await client.delete("counter")
    assert await client.decrby("counter", 5) == -5  # First decrement creates counter
    assert await client.decrby("counter", 3) == -8  # Second decrement
    assert await client.get("counter") == "-8"  # Verify value

    # Test decrement by negative value (increment)
    assert await client.decrby("counter", -3) == -5
    assert await client.get("counter") == "-5"

    # Test decrement by zero
    assert await client.decrby("counter", 0) == -5
    assert await client.get("counter") == "-5"

    # Test decrement on existing numeric value
    await client.set("counter", "10")
    assert await client.decrby("counter", 5) == 5
    assert await client.get("counter") == "5"

    # Test decrement on large numbers
    await client.set("counter", "10000000000")
    assert await client.decrby("counter", 1000) == 9999999000
    assert await client.get("counter") == "9999999000"

    # Test decrement on negative numbers
    await client.set("counter", "-5")
    assert await client.decrby("counter", 10) == -15
    assert await client.get("counter") == "-15"

    # Test decrement by negative value on negative number
    await client.set("counter", "-5")
    assert await client.decrby("counter", -10) == 5
    assert await client.get("counter") == "5"

    # Test decrement on non-numeric value
    await client.set("counter", "abc")
    with pytest.raises(ResponseError):
        await client.decrby("counter", 5)

    # Test decrement on non-existent key
    await client.delete("nonexistent_counter")
    assert await client.decrby("nonexistent_counter", 5) == -5
    assert await client.get("nonexistent_counter") == "-5"

    # Test decrement on empty string
    await client.set("counter", "")
    with pytest.raises(ResponseError):
        await client.decrby("counter", 5)

    # Test decrement on float value
    await client.set("counter", "1.5")
    with pytest.raises(ResponseError):
        await client.decrby("counter", 5)

    # Clean up test data
    await client.delete("counter", "nonexistent_counter")


async def test_pipeline_operations(client: NamespaceClient) -> None:
    """Test pipeline operations"""
    # Clean up any existing test data
    await client.delete(
        "key1", "key2", "key3", "hash_key", "list_key", "set_key", "zset_key"
    )

    # Test basic pipeline operations
    pipe = await client.pipeline()
    pipe.set(client.make_key("key1"), "value1")
    pipe.set(client.make_key("key2"), "value2")
    pipe.set(client.make_key("key3"), "value3")
    await pipe.execute()

    # Verify results
    assert await client.get("key1") == "value1"
    assert await client.get("key2") == "value2"
    assert await client.get("key3") == "value3"

    # Test pipeline with different data types
    pipe = await client.pipeline()
    pipe.hset(client.make_key("hash_key"), "field1", "value1")
    pipe.lpush(client.make_key("list_key"), "item1")
    pipe.sadd(client.make_key("set_key"), "member1")
    pipe.zadd(client.make_key("zset_key"), {"member1": 1.0})
    await pipe.execute()

    # Verify results
    assert await client.hget("hash_key", "field1") == "value1"
    assert await client.lrange("list_key", 0, -1) == ["item1"]
    assert await client.sismember("set_key", "member1")
    assert await client.zscore("zset_key", "member1") == 1.0

    # Test pipeline with error handling
    pipe = await client.pipeline()
    pipe.set(client.make_key("key1"), "new_value")
    pipe.incr(client.make_key("key1"))  # This will fail as key1 contains a string
    pipe.set(client.make_key("key2"), "value2")

    try:
        await pipe.execute()
    except ResponseError:
        # Verify partial execution
        assert await client.get("key1") == "new_value"  # First command should succeed
        assert await client.get("key2") == "value2"  # Third command should not execute

    # Test pipeline with empty commands
    pipe = await client.pipeline()
    result = await pipe.execute()
    assert result == []

    # Test pipeline with multiple operations on same key
    pipe = await client.pipeline()
    pipe.set(client.make_key("counter"), "1")
    pipe.incr(client.make_key("counter"))
    pipe.incr(client.make_key("counter"))
    pipe.get(client.make_key("counter"))
    result = await pipe.execute()
    assert result[-1] == "3"  # Final value should be 3

    # Test pipeline with special characters
    pipe = await client.pipeline()
    pipe.set(client.make_key("key:with:colon"), "value1")
    pipe.set(client.make_key("key with space"), "value2")
    pipe.set(client.make_key("key_with_unicode_中文"), "value3")
    await pipe.execute()

    assert await client.get("key:with:colon") == "value1"
    assert await client.get("key with space") == "value2"
    assert await client.get("key_with_unicode_中文") == "value3"

    # Test pipeline with large number of commands
    pipe = await client.pipeline()
    for i in range(1000):
        pipe.set(client.make_key(f"key{i}"), f"value{i}")
    await pipe.execute()

    # Verify some random keys
    assert await client.get("key0") == "value0"
    assert await client.get("key500") == "value500"
    assert await client.get("key999") == "value999"

    # Test pipeline with delete operations
    pipe = await client.pipeline()
    pipe.delete(client.make_key("key1"), client.make_key("key2"))
    pipe.exists(client.make_key("key1"))
    pipe.exists(client.make_key("key3"))
    result = await pipe.execute()
    assert result[1] == 0  # key1 should not exist
    assert result[2] == 1  # key3 should still exist

    # Clean up test data
    await client.delete(
        "key1",
        "key2",
        "key3",
        "hash_key",
        "list_key",
        "set_key",
        "zset_key",
        "key:with:colon",
        "key with space",
        "key_with_unicode_中文",
        "counter",
        *[f"key{i}" for i in range(1000)],
    )
