import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class NFSessionList implements Iterable<NFSession> {
   private List<NFSession> list;

   public NFSessionList() {
      list = new ArrayList<>();
   }

   public NFSessionList(Iterable<NFValue> values) {
      list = new ArrayList<>();
      NFSession session = new NFSession();
      for (NFValue val : values) {
         if (session.canAdd(val)) {
            session.add(val);
         } else {
            /* Since this record belongs to the other session,
             * we store the previous session first,
             * and then create a new session.
             */
            list.add(session);
            session = new NFSession(val);
         }
      }

      // Add the last session
      list.add(session);
   }

   @Override
   public Iterator<NFSession> iterator() {
      return list.iterator();
   }
}
