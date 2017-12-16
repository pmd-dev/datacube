package com.urbanairship.datacube.dbharnesses;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;

import org.apache.commons.lang.NotImplementedException;

import com.google.common.base.Optional;
import com.urbanairship.datacube.Address;
import com.urbanairship.datacube.Batch;
import com.urbanairship.datacube.DbHarness;
import com.urbanairship.datacube.Deserializer;
import com.urbanairship.datacube.IdService;
import com.urbanairship.datacube.Op;
import com.urbanairship.datacube.Util;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Transaction;


public class RedisDbHarness<T extends Op> implements DbHarness<T>
{
	private static final Future<?> nullFuture = new MapDbHarness.NullFuture();

	protected final Jedis jedis;
	protected final int ttlSeconds;
	protected final Deserializer<T> deserializer;
	protected final CommitType commitType;
	protected final IdService idService;

	public RedisDbHarness(Jedis jedis, int ttlSeconds, Deserializer<T> deserializer,
	                      CommitType commitType, IdService idService)
	{
		this.jedis = jedis;
		this.ttlSeconds = ttlSeconds;
		this.deserializer = deserializer;
		this.commitType = commitType;
		this.idService = idService;
	}

	public RedisDbHarness(Jedis jedis, Deserializer<T> deserializer,
	                      CommitType commitType, IdService idService)
	{
		this(jedis, -1, deserializer, commitType, idService);
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

			if(commitType == CommitType.READ_COMBINE_CAS) {
				Optional<byte[]> oldBytes;
				try {
					oldBytes = getRaw(address);
				} catch (Exception e) {
					throw new RuntimeException(e);
				}

				T newOp;
				if(oldBytes.isPresent()) {
					T oldOp = deserializer.fromBytes(oldBytes.get());
					newOp = (T)opFromBatch.add(oldOp);
				} else {
					newOp = opFromBatch;
				}

				set(redisKey, newOp);
			}
			else if(commitType == CommitType.OVERWRITE) {
				set(redisKey, opFromBatch);
			}
			else if (commitType == CommitType.INCREMENT) {
				increment(redisKey, opFromBatch);
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
		byte[] redisKey;
		try {
			redisKey = address.toWriteKey(idService);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}

		set(redisKey, op);
	}

	private void set(byte[] redisKey, T op)
	{
		if (ttlSeconds > 0)
		{
			jedis.setex(redisKey, ttlSeconds, op.serialize());
		}
		else
		{
			jedis.set(redisKey, op.serialize());
		}
	}

	private void increment(byte[] redisKey, T op)
	{
		long amount = Util.bytesToLong(op.serialize());

		if (ttlSeconds > 0)
		{
			Transaction trans = jedis.multi();
			trans.incrBy(redisKey, amount);
			trans.expire(redisKey, ttlSeconds);
			trans.exec();
		}
		else
		{
			jedis.incrBy(redisKey, amount);
		}
	}

	@Override
	public List<Optional<T>> multiGet(List<Address> addresses) throws IOException {
		throw new NotImplementedException();
	}

	@Override
	public void flush() throws InterruptedException {
		return; // all ops are synchronously applied, nothing to do
	}

	protected Optional<byte[]> getRaw(Address address) throws InterruptedException {
		byte[] redisKey;
		try {
			final Optional<byte[]> maybeKey = address.toReadKey(idService);
			if (maybeKey.isPresent()) {
				redisKey = maybeKey.get();
			} else {
				return Optional.absent();
			}
		} catch (Exception e) {
			throw new RuntimeException(e);
		}

		byte[] bytes = jedis.get(redisKey);

		if(bytes == null) {
			return Optional.absent();
		} else {
			return Optional.of(bytes);
		}
	}
}
