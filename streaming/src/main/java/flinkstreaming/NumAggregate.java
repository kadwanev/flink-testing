package flinkstreaming;

import java.util.ArrayList;
import java.util.stream.Collectors;

public class NumAggregate<T extends Number, U extends Number> {

    public T number;
    public ArrayList<U> reason = new ArrayList<>();

    public String toString() {
        String f = (number instanceof Double || number instanceof Float) ? "%f [%s]" : "%d [%s]";
        return String.format(f, this.number, this.reason.stream().map(Object::toString).collect(Collectors.joining(", ")));
    }

}
