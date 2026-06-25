package org.apache.iotdb.api.test.utils;

import com.github.javafaker.Faker;
import com.github.javafaker.service.FakeValuesService;
import com.github.javafaker.service.RandomService;
import org.apache.tsfile.utils.Binary;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.util.Locale;

/**
 * 用于构造不同类型的数据
 */
public class GenerateValues {
    private static final Faker faker = new Faker();
    private static final Faker fakerChinese = new Faker(new Locale("zh-CN"));

    public static int getInt() {
        return faker.number().randomDigit();
    }

    public static long getLong(int maxNumberOfDecimals) {
        return faker.number().randomNumber(maxNumberOfDecimals, false);
    }

    public static float getFloat(int maxNumberOfDecimals, int min, int max) {
        return (float) faker.number().randomDouble(maxNumberOfDecimals, min, max);
    }

    public static double getDouble(int maxNumberOfDecimals, int min, int max) {
        return faker.number().randomDouble(maxNumberOfDecimals, min, max);
    }

    public static boolean getBoolean() {
        return faker.bool().bool();
    }

    public static String getStringValue() {
        String zw = faker.name().nameWithMiddle();
        String alphanumeric = faker.bothify("???????####");
        return zw + alphanumeric;
    }

    public static long getTimeStamp(int maxNumberOfDecimals) {
        return faker.number().randomNumber(maxNumberOfDecimals, false);
    }

    public static Binary getBloB() {
        String zw = faker.name().nameWithMiddle();
        String alphanumeric = faker.bothify("???????####");
        return new Binary((zw+alphanumeric).getBytes(StandardCharsets.UTF_8));
    }

    public static LocalDate getDateValue() {
        // 注意：javafaker 的 numberBetween 上界是排他的，因此用 +1 保证能取到上界值；
        // 同时先确定年、月，再按该月实际天数选 day，避免生成 2 月 30 日、4 月 31 日等非法日期导致偶发失败。
        int year = faker.number().numberBetween(1000, 9999 + 1);
        int month = faker.number().numberBetween(1, 12 + 1);
        int maxDay = java.time.YearMonth.of(year, month).lengthOfMonth();
        int day = faker.number().numberBetween(1, maxDay + 1);
        return LocalDate.of(year, month, day);
    }

    public static String getString(int max) {
        FakeValuesService fakeValuesService = new FakeValuesService(
                new Locale("en-GB"), new RandomService());
        return fakeValuesService.regexify("[a-zA-Z_0-9]{" + max + "}");
    }

    public static String getCombinedCode() {
        return faker.code().asin();
    }

    public static String getNumberCode() {
        return faker.code().ean13();
    }

    public static String getChinese() {
        return fakerChinese.address().city();
    }

    public static void main(String[] args) {
        System.out.println(Integer.MAX_VALUE);

    }
}
