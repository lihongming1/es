package com.espro.jest.utils;

import com.fasterxml.jackson.databind.ObjectMapper;

public final class JsonUtils {

    private JsonUtils(){}

    private static ObjectMapper mapper = new ObjectMapper();

    public static String toJson(Object obj){
        String json = null;
        try{
            json = mapper.writeValueAsString(obj);
        }catch (Exception ex){
            ex.printStackTrace();
        }
        return json;
    }

}
