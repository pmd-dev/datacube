package com.urbanairship.datacube.ops;


import com.urbanairship.datacube.Deserializer;
import com.urbanairship.datacube.Op;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

public class LongOpWithIdTracker extends LongOp
{
	private final Set<Long> ids = new HashSet<Long>();
	public static final LongOpWithIdTrackerDeserializer DESERIALIZER = new LongOpWithIdTrackerDeserializer();

	public LongOpWithIdTracker(long val, Long id)
	{
		super(val);
		this.ids.add(id);
	}

	protected LongOpWithIdTracker(long val, Collection<Long> ids)
	{
		super(val);
		this.ids.addAll(ids);
	}

	@Override
	public Op add(Op otherOp)
	{
		if(!(otherOp instanceof LongOp))
		{
			throw new RuntimeException();
		}

		Set<Long> set = new HashSet<Long>();
		set.addAll(ids);
		set.addAll(((LongOpWithIdTracker)otherOp).ids);

		return new LongOpWithIdTracker(getLong() + ((LongOpWithIdTracker)otherOp).getLong(), set);
	}


	@Override
	public Op subtract(Op otherOp)
	{
		Set<Long> set = new HashSet<Long>();
		set.addAll(ids);
		set.removeAll(((LongOpWithIdTracker)otherOp).ids);

		return new LongOpWithIdTracker(getLong() - ((LongOpWithIdTracker)otherOp).getLong(), set);
	}

	@Override
	public byte[] serialize()
	{
		int size = (ids.size() + 2) * 8;
		ByteBuffer buffer = ByteBuffer.allocate(size).putLong(getLong());
		buffer.putLong(Long.valueOf(ids.size()));
		for (Long id: ids)
		{
			buffer.putLong(id);
		}

		return buffer.array();
	}

	public static class LongOpWithIdTrackerDeserializer implements Deserializer<LongOpWithIdTracker>
	{
		@Override
		public LongOpWithIdTracker fromBytes(byte[] bytes)
		{
			int length = bytes.length;
			if(length < 8)
			{
				throw new IllegalArgumentException("Input array was too small: " + Arrays.toString(bytes));
			}

			ByteBuffer buffer = ByteBuffer.wrap(bytes);
			long val = buffer.getLong();

			// Backwards compatibility with maps written out with LongOp and being read back in as LongOpWithIdTracker
			Set<Long> ids = new HashSet<Long>();
			if (length > 8)
			{
				long size = buffer.getLong();
				for (int i = 0; i < size; i++)
				{
					ids.add(buffer.getLong());
				}
			}

			return new LongOpWithIdTracker(val, ids);
		}
	}

	public Set<Long> getIds()
	{
		return ids;
	}
}
