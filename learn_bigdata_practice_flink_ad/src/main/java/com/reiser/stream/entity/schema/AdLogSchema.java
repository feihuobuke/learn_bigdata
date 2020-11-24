package com.reiser.stream.entity.schema;

import com.reiser.stream.entity.AdLog;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.io.IOException;

/**
 * @author: reiserx
 * Date:2020/11/17
 * Des:
 */
public class AdLogSchema implements DeserializationSchema<AdLog>, SerializationSchema<AdLog> {
    @Override
    public AdLog deserialize(byte[] bytes) throws IOException {
        return AdLog.parseFrom(bytes);
    }

    @Override
    public boolean isEndOfStream(AdLog adServerLog) {
        return false;
    }

    @Override
    public byte[] serialize(AdLog adServerLog) {
        return new byte[0];
    }

    @Override
    public TypeInformation<AdLog> getProducedType() {
        return TypeInformation.of(new TypeHint<AdLog>() {
        });
    }
}
