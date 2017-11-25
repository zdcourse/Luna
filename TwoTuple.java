package word_kmeans;


import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;

public class TwoTuple<A, B> {

    public final A first;

    public final B second;

    public TwoTuple(A a, B b){
        first = a;
        second = b;
    }

    @Override
    public int hashCode(){
        return new HashCodeBuilder(17, 31).
                append(first).
                append(second).
                toHashCode();
    }

    @Override
    public boolean equals(Object o) {

        if (!(o instanceof TwoTuple))
            return false;
        if (o == this)
            return true;
        TwoTuple tp = (TwoTuple)o;
        return new EqualsBuilder().
                append(first, tp.first).
                append(second, tp.second).
                isEquals();
    }
    public String toString(){
        return "(" + first + ", " + second + ")";
    }
}
