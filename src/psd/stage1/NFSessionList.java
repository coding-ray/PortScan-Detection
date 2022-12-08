package psd.stage1;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import psd.com.NFValue;

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
            boolean isSessionAddedToList = combineOrAdd(session, val);
            if (isSessionAddedToList)
               session = new NFSession(val);
         }
      }

      // Add the last session
      combineOrAdd(session, new NFValue());
   }

   private boolean combineOrAdd(NFSession session, NFValue value) {
      int index;

      if ((index = getAddableSessionIndex(value)) == -1) {
         list.add(session);
         return true; // Added to list. "session" is added to list
      }

      NFSession storedSession = list.get(index);
      storedSession.add(value);
      list.set(index, storedSession);
      return false; // Combined with the other session. "session" is not used.
   }

   private int getAddableSessionIndex(NFValue value) {
      for (int i = list.size() - 1; i >= 0; i--) {
         if (list.get(i).canAdd(value))
            return i;
      }
      return -1;
   }

   @Override
   public Iterator<NFSession> iterator() {
      return list.iterator();
   }
}

