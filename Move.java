package comp512st.paxos;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Objects;

public class Move implements Serializable {
//    public final int playerNum;
//    public final char direction; // e.g., 'U', 'D', 'L', 'R'

    public final Object val;
    public final long requestID;

    //public final int requestID;

//    public Move(int playerNum, char direction) {
//        this.playerNum = playerNum;
//        this.direction = direction;
//        //this.requestID = requestID;
//    }

    public Move(long id, Object val){
        this.val = val;
        this.requestID = id;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Move)) return false;
        Move other = (Move) o;
        return this.requestID == other.requestID &&
                Objects.equals(this.val, other.val);
    }

    @Override
    public int hashCode() {
        return Objects.hash(requestID, val);
    }

    @Override
    public String toString() {
        return "Move{id=" + requestID + ", val=" + Arrays.toString((Object[]) val) + "}";
    }
}
