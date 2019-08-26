package utils;

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import com.google.gson.stream.MalformedJsonException;

/**
 * Created by QIAN on 5/10/2015.
 */
public class Serialization {
    private static Gson mGSON= new GsonBuilder().excludeFieldsWithoutExposeAnnotation().create();
    public static String Serialize(Object obj)
    {
        return mGSON.toJson(obj);
    }
    public static <T> T Deserialize(String json, Class<T> cls)
    {
        return mGSON.fromJson(json,cls);
    }
    
    public static boolean isJsonValid(final String json)
            throws IOException {
        return isJsonValid(new StringReader(json));
    }

    private static boolean isJsonValid(final Reader reader)
            throws IOException {
        return isJsonValid(new JsonReader(reader));
    }

    private static boolean isJsonValid(final JsonReader jsonReader)
            throws IOException {
        try {
            JsonToken token;
            loop:
            while ( (token = jsonReader.peek()) != JsonToken.END_DOCUMENT && token != null ) {
                switch ( token ) {
                case BEGIN_ARRAY:
                    jsonReader.beginArray();
                    break;
                case END_ARRAY:
                    jsonReader.endArray();
                    break;
                case BEGIN_OBJECT:
                    jsonReader.beginObject();
                    break;
                case END_OBJECT:
                    jsonReader.endObject();
                    break;
                case NAME:
                    jsonReader.nextName();
                    break;
                case STRING:
                case NUMBER:
                case BOOLEAN:
                case NULL:
                    jsonReader.skipValue();
                    break;
                case END_DOCUMENT:
                    break loop;
                default:
                    throw new AssertionError(token);
                }
            }
            return true;
        } catch ( final MalformedJsonException ignored ) {
            return false;
        }
    } 

}
