package com.urbanairship.datacube.dbharnesses;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.Future;

import com.google.common.base.Optional;
import com.urbanairship.datacube.Address;
import com.urbanairship.datacube.Batch;
import com.urbanairship.datacube.DbHarness;
import com.urbanairship.datacube.Deserializer;
import com.urbanairship.datacube.IdService;
import com.urbanairship.datacube.Op;
import com.urbanairship.datacube.Util;

import redis.clients.jedis.Jedis;

public class PmdRedisDbHarness<T extends Op> extends RedisDbHarness<T>
{
	private static final Future<?> nullFuture = new MapDbHarness.NullFuture();

	public PmdRedisDbHarness(Jedis jedis, int ttlSeconds, Deserializer<T> deserializer,
	                      DbHarness.CommitType commitType, IdService idService)
	{
		super(jedis, ttlSeconds, deserializer, commitType, idService);
	}

	@Override
	public Future<?> runBatchAsync(Batch<T> batch, AfterExecute<T> afterExecute) throws FullQueueException
	{
		for(Map.Entry<Address,T> entry: batch.getMap().entrySet()) {
			Address address = entry.getKey();
			T opFromBatch = entry.getValue();

			byte[] redisKey;
			try {
				redisKey = address.toWriteKey(idService);
			} catch (Exception e) {
				throw new RuntimeException(e);
			}

			if (commitType == DbHarness.CommitType.INCREMENT) {
				byte[] bytes = opFromBatch.serialize();

				ByteBuffer buffer = ByteBuffer.wrap(bytes);

				long id = buffer.getLong();
				int weight = buffer.getInt();

				jedis.zadd(redisKey, weight, Util.longToBytes(id));
			}
			else {
				throw new AssertionError("Unsupported commit type: " + commitType);
			}
		}

		batch.reset();
		afterExecute.afterExecute(null); // null throwable => success
		return nullFuture;
	}

	@Override
	public Optional<T> get(Address address) throws IOException, InterruptedException
	{
		// TODO - need to read the weight and the entire set?  Is there a way to just get size?

		Optional<byte[]> bytes = getRaw(address);
		if(bytes.isPresent()) {
			return Optional.of(deserializer.fromBytes(bytes.get()));
		} else {
			return Optional.absent();
		}
	}

	@Override
	public void set(Address address, T op) throws IOException, InterruptedException
	{
		// TOOD


		byte[] redisKey;
		try {
			redisKey = address.toWriteKey(idService);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}

		set(redisKey, op);
	}

}
