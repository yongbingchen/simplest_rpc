@0xd30829ed4c0b7675;

struct RpcMessage {
    enum Type {
        request @0;
        response @1;
        event  @2;
    }

    type @0 :Type;
    id @1 :UInt16;
    requestCtx @2 :UInt32;
    payload @3 :Data;
}
