
import h5py
import sys
import os
import glob

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
		grp.create_dataset('/'+g+'/'+"stype", shape=(0,), 
				   dtype=np.int16,maxshape=(None,))
		grp.create_dataset('/'+g+'/'+"snum", shape=(0,), 
				   dtype=np.int16,maxshape=(None,))
		for d in f_in['/'+g].keys():
			name = '/'+g+'/'+d
			src = f_in[name]
			dset = grp.create_dataset(d, shape=(0,), 
						  dtype=src.dtype,maxshape=(None,))
			print name, dset.size, dset.shape
			
if __name__ == "__main__":

	if len(sys.argv)<2:
		print "bad args.  ", sys.argv[0], "dir_where_files_are"
		sys.exit(-1)
		
	f_in = h5py.File(fname_in,'r')
	f_out = h5py.File(fname_out,'a')
	copy_format(f_in, f_out)
	f_in.close()
	f_out.close()
