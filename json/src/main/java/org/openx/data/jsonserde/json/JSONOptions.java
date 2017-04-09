package org.openx.data.jsonserde.json;

/**
 * Created by nuvol on 4/7/2017.
 */
public class JSONOptions {
    boolean isCaseInsensitive = true;

    public boolean isCaseInsensitive() {
        return isCaseInsensitive;
    }

    public JSONOptions setCaseInsensitive(boolean caseInsensitive) {
        isCaseInsensitive = caseInsensitive;
        return this;
    }

    public static JSONOptions globalOptions = new JSONOptions();
}
