package flinkstreaming.aggregate;

import java.util.ArrayList;
import java.util.stream.Collectors;

public class NumAggregate<K, V extends Number, X extends Number> {

    public K key;
    public V number;
    public ArrayList<X> reason = new ArrayList<>();

    public String toString() {
        String f = (number instanceof Double || number instanceof Float) ? "(%s) Average: %.3f [%s]" : "(%s) Total: %d [%s]";
        return String.format(f, this.key.toString(), this.number, this.reason.stream().map(Object::toString).collect(Collectors.joining(", ")));
    }

}
