package server;

/**
 * Created by saifalharthi on 1/14/14.
 */
public class ConvertLitreal {

    public static String javaStringLiteral(String str)
    {
        StringBuilder sb = new StringBuilder("\"");
        for (int i=0; i<str.length(); i++)
        {
            char c = str.charAt(i);
            if (c == '\n')
            {
                sb.append("\\n");
            }
            else if (c == '\r')
            {
                sb.append("\\r");
            }
            else if (c == '"')
            {
                sb.append("\\\"");
            }
            else if (c == '\\')
            {
                sb.append("\\\\");
            }
            else if (c < 0x20)
            {
                sb.append(String.format("\\%03o", (int)c));
            }
            else if (c >= 0x80)
            {
                sb.append(String.format("\\u%04x", (int)c));
            }
            else
            {
                sb.append(c);
            }
        }
        sb.append("\"");
        return sb.toString();
    }

}
