package flinkstreaming.aggregate;

import java.util.ArrayList;
import java.util.stream.Collectors;

public class AllNumAggregate<V extends Number, X extends Number> {

    public V number;
    public ArrayList<X> reason = new ArrayList<>();

    public String toString() {
        String f = (number instanceof Double || number instanceof Float) ? "Average: %.3f [%s]" : "Total: %d [%s]";
        return String.format(f, this.number, this.reason.stream().map(Object::toString).collect(Collectors.joining(", ")));
    }

}
