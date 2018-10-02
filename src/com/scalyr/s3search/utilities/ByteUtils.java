package com.scalyr.s3search.utilities;

import java.lang.reflect.Field;

/**
 * Low-level memory manipulation utilities.
 */
@SuppressWarnings("sunapi")
public class ByteUtils {
  // we use the fully-qualified path to sun.misc.Unsafe so @SuppressWarnings works on it
  // (otherwise we'd get a compiler warning at the import statement above)
  private static final sun.misc.Unsafe UNSAFE;
  private static final int BYTE_ARRAY_OFFSET;
  public static final int BYTE_ARRAY_SCALE;
  @SuppressWarnings("unused")
  private static final int INT_ARRAY_OFFSET;
  @SuppressWarnings("unused")
  private static final int INT_ARRAY_SCALE;
  @SuppressWarnings("unused")
  private static final int SHORT_ARRAY_OFFSET;
  @SuppressWarnings("unused")
  private static final int SHORT_ARRAY_SCALE;

  static {
    try {
      Field theUnsafe = sun.misc.Unsafe.class.getDeclaredField("theUnsafe");
      theUnsafe.setAccessible(true);
      UNSAFE = (sun.misc.Unsafe) theUnsafe.get(null);
      BYTE_ARRAY_OFFSET = UNSAFE.arrayBaseOffset(byte[].class);
      BYTE_ARRAY_SCALE = UNSAFE.arrayIndexScale(byte[].class);
      INT_ARRAY_OFFSET = UNSAFE.arrayBaseOffset(int[].class);
      INT_ARRAY_SCALE = UNSAFE.arrayIndexScale(int[].class);
      SHORT_ARRAY_OFFSET = UNSAFE.arrayBaseOffset(short[].class);
      SHORT_ARRAY_SCALE = UNSAFE.arrayIndexScale(short[].class);
    } catch (IllegalAccessException e) {
      throw new ExceptionInInitializerError("Cannot access Unsafe");
    } catch (NoSuchFieldException e) {
      throw new ExceptionInInitializerError("Cannot access Unsafe");
    } catch (SecurityException e) {
      throw new ExceptionInInitializerError("Cannot access Unsafe");
    }
  }

  public static int getShortUnsafeLocalEndian(byte[] buffer, long offset) {
    return UNSAFE.getShort(buffer, BYTE_ARRAY_OFFSET + BYTE_ARRAY_SCALE * offset);
  }

  public static int getIntUnsafeLocalEndian(byte[] buffer, long offset) {
    return UNSAFE.getInt(buffer, BYTE_ARRAY_OFFSET + BYTE_ARRAY_SCALE * offset);
  }
}
