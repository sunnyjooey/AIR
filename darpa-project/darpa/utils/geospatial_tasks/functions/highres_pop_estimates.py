import numpy as np
import rasterio


def create_full_resolution_pop(
    county_pop_file,
    spatial_dist_file,
    fname_out="population_estimate.tif",
    fname_out2="population_estimate_2.tif",
    no_data_val=-9999,
):

    """Distributes county level population estimates to higher resolution grid

    This function distributes county level population estimates to a 1km^2 grid.

    Parameters
    ----------
    county_pop_file: Geotif file (*.tif)
        Raster file of county level population estimation.spatial_dist_file.

    spatial_dist_file: Geotif file (*.tif)
        Raster file with the gridded ratio of total county level population for each grid cell.

    no_data_val : float
        Value to assign no data points in output raster file.

    fname_out : string
        Path and file name for output raster.
    fname)_out2: optional for raster input with 2 bands
    References
    ----------

    Notes
    -----
    """
    # -------------------------------------
    # Open and read raster file with county
    # level population estimates
    # -------------------------------------
    fname_all = [fname_out, fname_out2]

    with rasterio.open(spatial_dist_file) as rastf:
        pop_dist = rastf.read()
        nodatasp = rastf.nodata
        prf = rastf.profile

    with rasterio.open(county_pop_file) as rastf:
        # loop thru bands
        bands = rastf.count
        # nodatacp=rastf.nodata
        for band in range(1, bands + 1):
            # band_num=band+1
            county_pop = rastf.read(band)
            # county_total_df.append(county_pop)
            nodatacp = rastf.nodata

            # --------------------------------------------------------------
            # Open and read raster file with spatial population distribution
            # --------------------------------------------------------------

            # for i in range(len(county_total_df)):
            # band_num=i+1
            # county_pop=rastf.read(1)
            # print('this is max: {}'.format(np.max(county_pop)))
            county_pop = np.squeeze(county_pop)
            pop_dist = np.squeeze(pop_dist)

            pop_est = np.ones(pop_dist.shape) * no_data_val
            ind1 = np.where(county_pop.flatten() != nodatacp)[0]
            ind2 = np.where(pop_dist.flatten() != nodatasp)[0]

            ind = np.intersect1d(ind1, ind2)
            ind2d = np.unravel_index(ind, pop_dist.shape)

            pop_est[ind2d] = county_pop[ind2d] * pop_dist[ind2d]
            pop_est[ind2d] = np.round(pop_est[ind2d])

            # Update raster meta-data
            prf.update(nodata=no_data_val)

            # Write out spatially distributed population estimate to raster
            with rasterio.open(fname_all[band - 1], "w", **prf) as out_raster:
                out_raster.write(pop_est.astype(rasterio.float32), 1)

                # out_raster.write(pop_est.astype('float32'), band_num)
