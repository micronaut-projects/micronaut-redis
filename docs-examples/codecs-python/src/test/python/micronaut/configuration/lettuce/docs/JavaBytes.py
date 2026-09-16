import java

# TODO(python): java.type needed because the primitive array type byte[] has no package to import it from
ByteArray = java.type("byte[]")


def java_bytes(text: str):
    """
    Encodes text as a Java byte[]: the RedisCommands<byte[], byte[]> methods are erased to Object
    parameters, so a Python bytes value passed to them would not be converted to a byte[].
    """
    data = text.encode("UTF-8")
    array = ByteArray(len(data))
    for index, byte in enumerate(data):
        array[index] = byte
    return array
