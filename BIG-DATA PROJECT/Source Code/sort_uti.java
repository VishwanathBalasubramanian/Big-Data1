package uber_proj.uber_proj;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class sort_uti{
	

public static <K, V extends Comparable<? super V>> HashMap<K, V> sortByValues( Map<K, V> map )
{
List<Map.Entry<K, V>> list =
    new LinkedList<Map.Entry<K, V>>( map.entrySet() );
Collections.sort( list, new Comparator<Map.Entry<K, V>>()
{
    public int compare( Map.Entry<K, V> o1, Map.Entry<K, V> o2 )
    {
        return (o2.getValue()).compareTo( o1.getValue() );
    }
} );

HashMap<K, V> result = new LinkedHashMap<K, V>();
for (Map.Entry<K, V> entry : list)
{
    result.put( entry.getKey(), entry.getValue() );
}
return result;
}

}