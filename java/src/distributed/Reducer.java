package distributed;

import java.util.List;

public interface Reducer<IK, IV, OK, OV> {
    
    Pair<OK, OV> reduce(IK key, List<IV> values);
}
