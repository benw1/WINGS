#! /usr/bin/env python
import wpipe as wp
import h5py
import vaex
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

def register(task):
    _temp = task.mask(source="*", name="start", value=task.name)
    _temp = task.mask(source="*", name="fake_hdf5_ready", value="*")

def export_and_plot_fraction(input_h5, output_txt, dset_path, col1, col2, col3,plot_png,
                             fill_val=99.999, n_bins=50):
    """
    1. Reads compound HDF5 dataset, writes ASCII with 3-decimal formatting
    2. Plots fraction of fill_val in col2 vs col1

    Column 2 = col2 if col3 is True AND col2 not NaN, else fill_val
    """
    print(repr(dset_path))
    df = pd.read_hdf(input_h5, key='data')
    ds = vaex.from_pandas(df)
    print("colnames ",ds.get_column_names())
    v1 = ds[col1].to_numpy()
    v2 = ds[col2].to_numpy()
    flag = ds[col3].to_numpy().astype(bool)
    #print("LENS: ",len(v1),len(v2))

    v2_out1 = np.where(flag & ~np.isnan(v2) & (np.fabs(v2-v1)<2.0), v2, fill_val)
    v2_out = np.where((np.fabs(v2_out1-v1)<2.0), v2_out1-v1, v2_out1)
    v1_out = np.where(np.isnan(v1), fill_val, v1)
    print("LENS: ",len(v1_out),len(v2_out))
    # Write ASCII file, 3 decimal places
    with open(output_txt, 'w') as f:
        f.write(f"# {col1} {col2}_conditional\n")
        for a, b in zip(v1_out, v2_out):
            f.write(f"{a:.3f} {b:.3f}\n")
    print("wrote ",output_txt)
    # Bin by col1 and compute fraction of fill_val
    bins = np.linspace(np.min(v1_out), np.max(v1_out), n_bins + 1)
    bin_idx = np.digitize(v1_out, bins) - 1 # which bin each point falls in

    bin_centers = 0.5 * (bins[:-1] + bins[1:])
    fractions = np.zeros(n_bins)

    for i in range(n_bins):
        in_bin = bin_idx == i
        if np.any(in_bin):
            fractions[i] = 1-np.mean(np.isclose(v2_out[in_bin], fill_val))
        else:
            fractions[i] = np.nan # no data in this bin

    # Plot
    plt.figure(figsize=(8, 5))
    plt.plot(bin_centers, fractions, 'o-', ms=4)
    plt.xlabel(f"{col1}")
    plt.ylabel(f"Completeness Fraction")
    plt.title(f"Completeness for {col1}")
    plt.ylim(-0.05, 1.05)
    plt.grid(True, alpha=0.3)
    plt.tight_layout()
    plt.savefig(plot_png,bbox_inches='tight')
    print("wrote ",plot_png)
    return v1_out, v2_out, bin_centers, fractions

# Example:


if __name__ == "__main__":
    my_pipe = wp.Pipeline()
    my_job = wp.Job()
    my_job.logprint("processing phot file...")
    my_config = my_job.config
    my_target = my_job.target
    this_event = my_job.firing_event
    my_job.logprint(this_event)
    my_job.logprint(this_event.options)
    my_config = my_job.config
    logpath = my_config.logpath
    procpath = my_config.procpath
    fake_dp_id = this_event.options['dp_id']
    fake_dp = wp.DataProduct(fake_dp_id)
    fake_hdf5 = fake_dp.filename
    filters = my_config.parameters["det_filters"].split(",")
    for filt in filters:
        #filt_fake = "nircam_f115w"
        my_job.logprint(f"sending {fake_hdf5}, {filt.strip()}.matchfake, {filt.strip().lower()}_vega")
        v1, v2, x, frac = export_and_plot_fraction(
            input_h5=fake_hdf5,
            output_txt=my_config.procpath+"/"+filt.strip()+'.matchfake',
            dset_path='data/table',
            #col1='NIRCAM_F115W_mag_in',
            #col2='nircam_f115w_vega',
            #col3='nircam_f115w_gst',plot_png=filt_fake+'_comp.png',
            col1 = filt.strip() + "_mag_in",
            col2 = filt.strip().lower() + "_vega",
            col3 = filt.strip().lower() + "_gst",
            plot_png= my_config.procpath+"/"+filt.strip().lower() + "_comp.png",
            n_bins=40
        ) 

