
import h5py
import sys
import os
import glob
import numpy as np

# in_dir = '/scratch/ssehrish/h5out/TTJets_13TeV_amcatnloFXFX_pythia8_2'
# out_dir = '/scratch2/jbk'

# fname_out = out_dir + '/' + 'TTJets_13TeV_amcatnloFXFX_pythia8_2_all.h5'
# fname_in = in_dir + '/' + 'TTJets_13TeV_amcatnloFXFX_pythia8_2_10.h5'

# print fname_out
# print fname_in

def copy_format(f_in, f_out):
	for g in f_in.keys():
		print g
		grp = f_out.create_group(g)
        	# create datasets for sample type and index
		#grp.create_dataset('/'+g+'/'+"stype", shape=(0,), 
		#		   dtype=np.int16,maxshape=(None,))
		#grp.create_dataset('/'+g+'/'+"snum", shape=(0,), 
		#		   dtype=np.int16,maxshape=(None,))
		for d in f_in['/'+g].keys():
			name = '/'+g+'/'+d
                        print name
			src = f_in[name]
			dset = grp.create_dataset(d, shape=(0,), 
						  dtype=src.dtype,maxshape=(None,))
			print name, dset.size, dset.shape


def make_prototype(in_dir, out_dir):
    flist = glob.glob(in_dir + "/*")
    if len(flist)==0:
        print "no files in ",in_dir
        sys.exit(-1)

    fname_in=flist[0]
    fname_out=out_dir + os.path.basename(os.path.dirname(in_dir)) + '_all.h5'
    print fname_in, fname_out, in_dir
    
    f_in = h5py.File(fname_in,'r')
    f_out = h5py.File(fname_out,'a')
    
    copy_format(f_in, f_out)
    
    f_in.close()
    f_out.close()
    
    return fname_out
    

if __name__ == "__main__":
    
    if len(sys.argv)<3:
        print "bad args.  ", sys.argv[0], "dir_where_files_are out_dir"
        sys.exit(-1)
    
    in_dir = sys.argv[1]
    out_dir = sys.argv[2]

    outfile = make_prototype(in_dir, out_dir)
    print "copied format to file",outfile
    
