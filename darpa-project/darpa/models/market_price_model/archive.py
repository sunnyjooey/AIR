# # Moved login in the data_cleaning.py from the following function to PullCHIRPSCKAN to account for any changing that might happen
# # When scraping from the website.
# class ScrapeRainfallData(ExternalTask):
#     """
#     Pull CHIRPS monthly rainfall data from ftp, unzip files
#     """
#
#     def output(self):
#         return IntermediateTarget(task=self, timeout=60 * 60 * 24 * 365)
#
#     def run(self):
#         outfolder = os.path.join(
#             CONFIG.get("paths", "output_path"), "marketprice_model_chirps_dir"
#         )
#
#         if not os.path.exists(outfolder):
#             os.makedirs(outfolder)
#
#         # connect to FTP
#         ftp = FTP("ftp.chc.ucsb.edu")  # noqa: S321 - ignore secuirty check
#         ftp.sendcmd("USER anonymous")
#         ftp.sendcmd("PASS anonymous@")
#
#         # go to Africa monthly tifs
#         ftp.cwd("pub/org/chc/products/CHIRPS-2.0/africa_monthly/tifs/")
#         files = ftp.nlst()
#
#         # only download files for relevant year
#         download = [f for f in files if "201" in f]
#         for f in download:
#             if not os.path.isfile(os.path.join(outfolder, f.replace(".gz", ""))):
#                 ftp.retrbinary(
#                     "RETR " + f, open(os.path.join(outfolder, f), "wb").write
#                 )
#
#         # unzip files, return paths
#         os.system("gunzip {}/*.gz".format(outfolder))  # noqa: S605
#
#         # For the files that didn't unzip for some reason, re-download and unzip them
#         for f in [i for i in os.listdir(outfolder) if i.endswith(".gz")]:
#             ftp.retrbinary("RETR " + f, open(os.path.join(outfolder, f), "wb").write)
#             os.system("gunzip {}".format(os.path.join(outfolder, f)))  # noqa: S605
#
#         full_paths = {
#             f: os.path.join(outfolder, f) for f in sorted(os.listdir(outfolder))
#         }
#         with self.output().open("w") as output:
#             output.write(full_paths)
