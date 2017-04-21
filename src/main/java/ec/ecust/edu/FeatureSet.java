package ec.ecust.edu;

import java.util.HashSet;

/**
 * Created by pengfei on 2017/4/21.
 */
public class FeatureSet<E> extends HashSet{
    public int compare(FeatureSet<E> set){
        int num = 0 ;
        for(Object e:set){
            if(contains(e)){
                num++ ;
            }
        }
        return num ;
    }
}
