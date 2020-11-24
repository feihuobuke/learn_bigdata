package com.reiser.stream.entity.schema;

import com.reiser.stream.entity.AdServerLog;
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
public class AdServiceLogSchema implements DeserializationSchema<AdServerLog>, SerializationSchema<AdServerLog> {
    @Override
    public AdServerLog deserialize(byte[] bytes) throws IOException {
        return AdServerLog.parseFrom(bytes);
    }

    @Override
    public boolean isEndOfStream(AdServerLog adServerLog) {
        return false;
    }

    @Override
    public byte[] serialize(AdServerLog adServerLog) {
        return new byte[0];
    }

    @Override
    public TypeInformation<AdServerLog> getProducedType() {
        return TypeInformation.of(new TypeHint<AdServerLog>() {
        });
    }
}
