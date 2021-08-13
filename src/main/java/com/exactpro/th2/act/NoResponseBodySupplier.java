package com.exactpro.th2.act;

import java.util.Collection;

import com.exactpro.th2.common.event.IBodyData;

public interface NoResponseBodySupplier {
    Collection<IBodyData> createNoResponseBody();
}
