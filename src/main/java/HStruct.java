import ncsa.hdf.hdf5lib.H5;
import ncsa.hdf.hdf5lib.HDF5Constants;
import java.util.*;
import ncsa.hdf.hdf5lib.structs.H5G_info_t;
import ncsa.hdf.hdf5lib.structs.H5O_info_t;
import ncsa.hdf.hdf5lib.callbacks.H5L_iterate_cb;
import ncsa.hdf.hdf5lib.callbacks.H5L_iterate_t;
import ncsa.hdf.hdf5lib.structs.H5L_info_t;
import ncsa.hdf.hdf5lib.callbacks.H5O_iterate_cb;
import ncsa.hdf.hdf5lib.callbacks.H5O_iterate_t;

public class HStruct {
    /*****************************************
    **
    **	Data Structures
    **
    **    currently using a Vector. It was easy to implement.
    **    I am not sure how effecient Vectors are for searching
    **    and appending to, so there is probably a better solution.
    **    What ever data type we use needs to allow us to continually
    **    change the size
    **
    *****************************************/   
    private Vector ds = new Vector(0);


    /****************************************
    **
    **  public static void main(String args[]) throws Exception
    **
    **    Main class
    **
    ****************************************/
    /*public static void main(String args[]) throws Exception {
	System.out.println("Running");
        HStruct test = new HStruct();
        try{
long time1= System.nanoTime();
            test.mapHDF5("/Users/abuchan/Documents/programming/java");
long time2= System.nanoTime() - time1;
System.out.println("\t\t\t\t\t\tTotal Time to discover: " + time2);
            //test.mapHDF5("/global/cscratch1/sd/abuchan/testData");
        } catch (Exception e){
            //Error
	    System.out.println("Failed reading file: " + e.getMessage());
        }
//test.dumpData();
	System.out.println("Done");
    }*/
    
    
    /****************************************
    **
    **  public void mapHDF5(String dname) throws Exception
    **
    **    call this instead of the recursive functions directly. 
    **    This only takes one parameter and opens the file desc
    **
    ****************************************/
    public void mapHDF5(String dname) throws Exception{
      int file_id = H5.H5Fopen("test.h5", HDF5Constants.H5F_ACC_RDWR, HDF5Constants.H5P_DEFAULT);
      mapHDF5_E(file_id, "/");
    }


    /****************************************
    **
    **  public void dumpData()
    **
    **    Dumps memory to screen
    **
    ****************************************/ 
    public void dumpData(){
	System.out.println("Size: " + ds.size());
	Enumeration vEnum = ds.elements();
        System.out.println("\nElements in vector:");
      
      	while(vEnum.hasMoreElements())
            System.out.println(vEnum.nextElement() + " ");
    }
    /****************************************
    **
    **  public void addPath(String path)
    **
    **    Each of the mapping functions calls this when
    **    a new data set is found. This allows us to easily
    **    switch between datatypes and not have to change 
    **    all the mapping functions.
    **
    ****************************************/
    private void addPath(String path){
	ds.addElement(new String(path));
    }



    /****************************************
    **
    **  public void mapHDF5_A(int fd, String path) throws Exception
    **
    **    Calls:  H5.H5Gopen
    **    	  H5.H5Gget_info
    **  	  H5.H5Gget_obj_info_all
    **		  mapHDF5_A
    **		  addPath
    **
    **	  12MB sample:	0.0473s
    **
    ****************************************/
    public void mapHDF5_A(int fd, String path) throws Exception{

        //Find number of children and create appropriate arrays

        //get id of group pointed to by path
        int gsetid = H5.H5Gopen(fd, path, HDF5Constants.H5P_DEFAULT);

        //get info about the group
        H5G_info_t ginfo = H5.H5Gget_info(gsetid);

        //create arrays to be filled in next step
        String objNames[]= new String[(int)ginfo.nlinks];
        int objTypes[]   = new int[(int)ginfo.nlinks];
        int lnkTypes[]   = new int[(int)ginfo.nlinks];
        long objRefs[]   = new long[(int)ginfo.nlinks];

        //fill arrays with info about the groups children
        int names_found = H5.H5Gget_obj_info_all(fd, path, objNames, objTypes, lnkTypes, objRefs, HDF5Constants.H5_INDEX_NAME);
        //Iterate over all the children in current group
        for(int x = 0; x <  ginfo.nlinks; x++){
            if(objTypes[x] == HDF5Constants.H5O_TYPE_GROUP){

                //This child is another group
                mapHDF5_A(fd, path + objNames[x] + "/");

            } else if(objTypes[x] == HDF5Constants.H5O_TYPE_DATASET){

                //This child is a dataset
                //TODO: append data to vector
                //println("Found: " + path + objNames(x))
                //ds += path + objNames[x]
		addPath(path + objNames[x]);
            } else{
                 //Error
            }
        }
    }

    /****************************************
    **
    **  public void mapHDF5_B(int fd, String path) throws Exception
    **
    **    Calls:  H5.H5Gopen
    **            H5.H5Gget_objname_by_idx
    **            H5.H5Gopen
    **            mapHDF5_B
    **            addPath
    **
    **	  12MB sample:	0.0974s
    **
    ****************************************/
    public void mapHDF5_B(int fid, String path) throws Exception{
        int gsetid = H5.H5Gopen(fid, path, HDF5Constants.H5P_DEFAULT);
        String objname[]= new String[30];
        Long objlen = 30L;
        Long idx = 0L;
        Long res = 1L;
        while(res != 0){
            try{
                res = H5.H5Gget_objname_by_idx(gsetid,idx,objname, objlen);
            }catch (Exception e){
                //println(e.printStackTrace())
                //println(e.toString())
                return; //break

            }

            try{
                int t =H5.H5Gopen(fid, path + objname[0] + "/", HDF5Constants.H5P_DEFAULT);
                mapHDF5_B(fid, path + objname[0] + "/");
            }catch (Exception e){
                //println(e.toString())
		addPath(path + objname[0]);
                //System.out.println("found: " + path + objname[0]);
            }


        idx = idx + 1;
      }
    }


    /****************************************
    **
    **  public void mapHDF5_C(int fd, String path) throws Exception
    **  
    **    Calls:  H5.H5Gopen
    **            H5.H5Gget_objname_by_idx
    **            H5.H5Gget_objtype_by_idx
    **            mapHDF5_C
    **            addPath
    **
    **	  12MB sample: 0.0848s
    **
    ****************************************/
    public void mapHDF5_C(int fid, String path) throws Exception{
        int gsetid = H5.H5Gopen(fid, path, HDF5Constants.H5P_DEFAULT);
        String objname[] = new String[30];
        Long objlen = 30L;
        Long idx = 0L;
        Long res = 1L;
        //Iterate through all the children
        while(res != 0){
            //get child name
            try{
                res = H5.H5Gget_objname_by_idx(gsetid,idx,objname, objlen);
            }catch (Exception e){
                //println(e.printStackTrace())
                //println(e.toString())
                return; //break
            }

            //get child type
            int otype = H5.H5Gget_objtype_by_idx(gsetid, idx);
            if(otype == HDF5Constants.H5O_TYPE_GROUP){

                //This child is another group
                mapHDF5_C(fid, path + objname[0] + "/");

            } else if(otype==HDF5Constants.H5O_TYPE_DATASET){

                //This child is a dataset
                //println("Found: " + path + objNames(x))
                //ds += path + objname[0]
		addPath(path + objname[0]);
            } else{
                        //Error
            }

        idx = idx + 1;
        }
  }

  /****************************************
  **
  **  public void mapHDF5_D(int fid, String path) throws Exception
  **
  **    Calls:  H5.H5Oget_info
  **    	H5.H5Literate
  **		H5L_iter_callback_O.callback
  **
  **	12MB sample: 0.0606sec
  **
  ****************************************/
  public void mapHDF5_D(int fid, String path) throws Exception{
    opdata od = new opdata();
    H5O_info_t infobuf = H5.H5Oget_info(fid);
    od.recurs = 0;
    od.prev = null;
    od.addr = infobuf.addr;

    H5L_iterate_cb cb = new H5L_iter_callback_L();
    H5.H5Literate(fid, HDF5Constants.H5_INDEX_NAME, HDF5Constants.H5_ITER_NATIVE, 0L, cb, od);
  }
  /****************************************
  **
  **  public void mapHDF5_E(int fid, String path) throws Exception
  **
  **    Calls:  H5.H5Oget_info
  **            H5.H5Ovisit
  **            H5L_iter_callback_L.callback
  **
  **    12MB sample: 0.0487sec
  **
  ****************************************/
  public void mapHDF5_E(int fid, String path) throws Exception{
    H5O_iterate_t od = new H5O_iter_data();
    H5O_iterate_cb cb = new H5L_iter_callback_O();
    H5.H5Ovisit(fid, HDF5Constants.H5_INDEX_NAME, HDF5Constants.H5_ITER_INC, cb, od);
  }
}



/*******************	IGNORE BELOW THIS POINT	    *****************************/


  /****************************************
  **
  **	public int callback(int group, String name, H5O_info_t info, H5O_iterate_t op_data)
  **	class H5O_iter_data implements H5O_iterate_t  
  **
  **	Classes and functions used by mapHDF5_E
  **
  ****************************************/
class H5L_iter_callback_O implements H5O_iterate_cb {
        public int callback(int group, String name, H5O_info_t info, H5O_iterate_t op_data) {
                //System.out.println("Group: " + group + "\nName: " + name + "\n"); 
                return 0;
        }
}
class H5O_iter_data implements H5O_iterate_t {
            int recurs;
        opdata prev;
        long addr;
}


  /****************************************
  **
  **    public int callback(int group, String name, H5O_info_t info, H5O_iterate_t op_data)
  **    class H5L_iter_data implements H5L_iterate_t
  **
  **    Classes and functions used by mapHDF5_D
  **
  ****************************************/
class opdata implements H5L_iterate_t {
        int recurs;
        opdata prev;
        long addr;
}
class H5L_iter_callback_L implements H5L_iterate_cb {
	public int callback(int group, String name, H5L_info_t info, H5L_iterate_t op_data) {
		System.out.println("Group: " + group + "\nName: " + name + "\n");
		
		try{
			H5O_info_t infobuf = H5.H5Oget_info_by_name (group, name, HDF5Constants.H5P_DEFAULT);
			if(infobuf.type == HDF5Constants.H5O_TYPE_GROUP){
				opdata od = new opdata();
    				//H5O_info_t infobuf = H5.H5Oget_info(group);
    				od.recurs = 0;
    				od.prev = null;
    				od.addr = infobuf.addr;
				H5L_iterate_cb cb = new H5L_iter_callback_L();
				H5.H5Literate_by_name (group, name, HDF5Constants.H5_INDEX_NAME,
                                                      HDF5Constants.H5_ITER_NATIVE, 0L, cb,od,
                                                      HDF5Constants.H5P_DEFAULT);
			}
		}catch (Exception e){
        	    //Error
        	    //System.out.println("Failed to iterate: " + e.getMessage());
       		}
		
		return 0;
	}
}
