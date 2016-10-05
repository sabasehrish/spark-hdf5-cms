
import h5py

in_dir = '/scratch/ssehrish/h5out/TTJets_13TeV_amcatnloFXFX_pythia8_2'
out_dir = '/scratch2/jbk'

fname_out = out_dir + '/' + 'TTJets_13TeV_amcatnloFXFX_pythia8_2_all.h5'
fname_in = in_dir + '/' + 'TTJets_13TeV_amcatnloFXFX_pythia8_2_10.h5'

print fname_out
print fname_in

f_in = h5py.File(fname_in,'r')
f_out = h5py.File(fname_out,'a')

for g in f_in.keys():
	print g
	grp = f_out.create_group(g)
	for d in f_in['/'+g].keys():
		name = '/'+g+'/'+d
		src = f_in[name]
		dset = grp.create_dataset(d, shape=(0,), dtype=src.dtype,maxshape=(None,))
		print name, dset.size, dset.shape

f_in.close()
f_out.close()
