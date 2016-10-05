
import h5py
import sys

in_dir = '/scratch/ssehrish/h5out/TTJets_13TeV_amcatnloFXFX_pythia8_2'
out_dir = '/scratch2/jbk'

if len(sys.argv)<2: 
	print "wrong arguments, give in and out (in that order"
	sys.exit(-1)

print sys.argv[1], sys.argv[2]

# fname_out = out_dir + '/' + 'TTJets_13TeV_amcatnloFXFX_pythia8_2_all.h5'
# fname_in = in_dir + '/' + 'TTJets_13TeV_amcatnloFXFX_pythia8_2_0.h5'

fname_out = out_dir + '/' + sys.argv[2]
fname_in = in_dir + '/' + sys.argv[1]

print fname_out
print fname_in

f_in = h5py.File(fname_in,'r')
f_out = h5py.File(fname_out,'a')

for g in f_in.keys():
	print g
	for d in f_in['/'+g].keys():
		name = '/'+g+'/'+d
		dset_src = f_in[name]
		dset_dst = f_out[name]
		#print name, dset_dst.size, dset_src.size,dset_dst.shape
		old_size = dset_dst.size
		new_size = dset_dst.size+dset_src.size
		dset_dst.resize( (new_size,) )
		dset_dst[old_size:new_size] = dset_src
		#print name, dset_dst.size, dset_dst.shape

f_in.close()
f_out.close()
