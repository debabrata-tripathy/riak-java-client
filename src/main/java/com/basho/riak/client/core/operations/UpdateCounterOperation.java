/*
 * Copyright 2013 Basho Technologies Inc
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.basho.riak.client.core.operations;

import com.basho.riak.client.StoreMeta;
import com.basho.riak.client.core.FutureOperation;
import com.basho.riak.client.core.RiakMessage;
import com.basho.riak.client.util.ByteArrayWrapper;
import com.basho.riak.client.util.RiakMessageCodes;
import com.basho.riak.protobuf.RiakKvPB;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

import java.util.concurrent.ExecutionException;

import static com.basho.riak.client.core.operations.Operations.checkMessageType;

/**
 * An operation to update a Riak counter.
 */
public class UpdateCounterOperation extends FutureOperation<Long>
{

	private final ByteArrayWrapper bucket;
	private final ByteArrayWrapper key;
	private final long amount;
	private StoreMeta storeMeta;

	public UpdateCounterOperation(ByteArrayWrapper bucket, ByteArrayWrapper key, long amount)
	{
		this.bucket = bucket;
		this.key = key;
		this.amount = amount;
	}

	/**
	 * The {@link StoreMeta} to use for this fetch operation
	 *
	 * @param storeMeta
	 * @return
	 */
	public UpdateCounterOperation withStoreMeta(StoreMeta storeMeta)
	{
		this.storeMeta = storeMeta;
		return this;
	}

	@Override
	protected Long convert(RiakMessage rawResponse) throws ExecutionException
	{

		checkMessageType(rawResponse, RiakMessageCodes.MSG_CounterGetResp);

		RiakKvPB.RpbCounterUpdateResp response = null;
		try
		{
			response = RiakKvPB.RpbCounterUpdateResp.parseFrom(rawResponse.getData());
		}
		catch (InvalidProtocolBufferException e)
		{
			throw new ExecutionException(e);
		}

		return response.hasValue() ? response.getValue() : null;
	}

	@Override
	protected RiakMessage createChannelMessage()
	{

		RiakKvPB.RpbCounterUpdateReq.Builder builder = RiakKvPB.RpbCounterUpdateReq.newBuilder();
		builder.setBucket(ByteString.copyFrom(bucket.unsafeGetValue()));
		builder.setKey(ByteString.copyFrom(key.unsafeGetValue()));

		if (storeMeta.hasW())
		{
			builder.setW(storeMeta.getW().getIntValue());
		}

		if (storeMeta.hasDw())
		{
			builder.setDw(storeMeta.getDw().getIntValue());
		}

		if (storeMeta.hasPw())
		{
			builder.setPw(storeMeta.getPw().getIntValue());
		}

		builder.setAmount(amount);

		RiakKvPB.RpbCounterUpdateReq req = builder.build();
		return new RiakMessage(RiakMessageCodes.MSG_CounterUpdateReq, req.toByteArray());

	}
}
