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
import com.basho.riak.client.cap.ConflictResolver;
import com.basho.riak.client.convert.Converter;
import com.basho.riak.client.convert.Converters;
import com.basho.riak.client.core.FutureOperation;
import com.basho.riak.client.core.RiakMessage;
import com.basho.riak.client.core.converters.PutRespConverter;
import com.basho.riak.client.query.RiakObject;
import com.basho.riak.client.util.ByteArrayWrapper;
import com.basho.riak.client.util.RiakMessageCodes;
import com.basho.riak.protobuf.RiakKvPB;
import com.google.protobuf.ByteString;

import java.util.List;
import java.util.concurrent.ExecutionException;

/**
 * An operation to store a riak object
 *
 * @param <T> the user type of the operation to store
 */
public class StoreOperation<T> extends FutureOperation<T>
{

	private final ByteArrayWrapper bucket;
	private final ByteArrayWrapper key;
	private ConflictResolver<T> conflictResolver;
	private Converter<T> domainObjectConverter;
	private StoreMeta storeMeta = StoreMeta.newBuilder().build();

	public StoreOperation(ByteArrayWrapper bucket, ByteArrayWrapper key)
	{
		this.bucket = bucket;
		this.key = key;
	}

	/**
	 * A {@link Converter} to use to convert the data fetched to some other type
	 *
	 * @param domainObjectConverter the converter to use.
	 * @return this
	 */
	public StoreOperation<T> withConverter(Converter<T> domainObjectConverter)
	{
		this.domainObjectConverter = domainObjectConverter;
		return this;
	}

	/**
	 * The {@link StoreMeta} to use for this fetch operation
	 *
	 * @param fetchMeta
	 * @return this
	 */
	public StoreOperation<T> withStoreMeta(StoreMeta fetchMeta)
	{
		this.storeMeta = fetchMeta;
		return this;
	}

	/**
	 * The {@link ConflictResolver} to use resole conflicts on fetch, if they exist
	 *
	 * @param resolver
	 * @return
	 */
	public StoreOperation<T> withResolver(ConflictResolver<T> resolver)
	{
		this.conflictResolver = resolver;
		return this;
	}

	@Override
	protected T convert(RiakMessage rawResponse) throws ExecutionException
	{
		PutRespConverter responseConverter = new PutRespConverter(bucket, key);
		List<RiakObject> riakObjects = responseConverter.convert(rawResponse);
		List<T> domainObjects = Converters.convert(domainObjectConverter, riakObjects);
		return conflictResolver.resolve(domainObjects);
	}

	@Override
	protected RiakMessage createChannelMessage()
	{

		RiakKvPB.RpbPutReq.Builder builder = RiakKvPB.RpbPutReq.newBuilder();
		builder.setBucket(ByteString.copyFrom(bucket.unsafeGetValue()));
		builder.setKey(ByteString.copyFrom(bucket.unsafeGetValue()));

		if (storeMeta.hasAsis())
		{
			builder.setAsis(storeMeta.getAsis());
		}

		if (storeMeta.hasDw())
		{
			builder.setDw(storeMeta.getDw().getIntValue());
		}

		if (storeMeta.hasIfNoneMatch())
		{
			builder.setIfNoneMatch(storeMeta.getIfNoneMatch());
		}

		if (storeMeta.hasPw())
		{
			builder.setPw(storeMeta.getPw().getIntValue());
		}

		if (storeMeta.hasIfNotModified())
		{
			builder.setIfNotModified(storeMeta.getIfNotModified());
		}

		if (storeMeta.hasReturnBody())
		{
			builder.setReturnBody(storeMeta.getReturnBody());
		}

		if (storeMeta.hasTimeout())
		{
			builder.setTimeout(storeMeta.getTimeout());
		}

		if (storeMeta.hasReturnHead())
		{
			builder.setReturnHead(storeMeta.getReturnHead());
		}

		if (storeMeta.hasW())
		{
			builder.setW(storeMeta.getW().getIntValue());
		}

		if (storeMeta.hasNval())
		{
			builder.setNVal(storeMeta.getNval());
		}

		if (storeMeta.hasSloppyQuorum())
		{
			builder.setSloppyQuorum(storeMeta.getSloppyQuorum());
		}

		RiakKvPB.RpbPutReq req = builder.build();
		return new RiakMessage(RiakMessageCodes.MSG_PutReq, req.toByteArray());

	}


}
