FROM cvillalobosuf/dssat-csm:develop AS dssat

# overkill to copy only a single script, but I don't want to include it in our open source
# codebase without permisson. It is already public within the Docker image where we will
# leave it
FROM cvillalobosuf/peerless:1.0.0 AS peerless

# we are building our image based on a jupyterlab provided image
# tensorflow image doesn't work, because we need version 2.3.* of tensorflow,
# conda only provides up to 2.2.0 currently. So we need to install it through pip
FROM jupyter/scipy-notebook:42f4c82a07ff

# copy app directory from cvillalobosuf/dssat-csm:develop image
COPY --from=dssat /app /app

# copy peerless script from cvillalobosuf/peerless image
COPY --from=peerless /run/peerless.py /run/peerless.py

# install conda packages
# only copying in conda requirements now, so we don't have to rebuild
# this step every time e.g. pip requirements file changes
COPY requirements/base.conda /tmp/requirements/base.conda
RUN conda install --yes --quiet mamba && \
    mamba install --yes --quiet --file /tmp/requirements/base.conda && \
    conda clean --all --yes --quiet

# install pip packages
COPY requirements/base.pip /tmp/requirements/base.pip
RUN pip install --use-feature=2020-resolver --requirement /tmp/requirements/base.pip
# descarteslabs has a dependency on pyarrow==0.17.1 but we need a newer version
# this should be moved to base.conda as soon as descarteslabs has upgraded this
# requirement
# We need pandas 1.0.0 for xarray, but one pip-installed package upgrades it.
# So we have to downgrade it here as well - see MODMAP-156
RUN pip install --use-feature=2020-resolver pyarrow==2.0.0 pandas==1.0.0

# the name of the user created in the jupyter image is called jovyan
# we need to change to the privileged user to be able to change ownership
# In the entrypoint script, we also need to change the UID of the jovyan
# user to the UID of the host system's user, so mounts are writable (when 
# the UID is e.g. 1001). We use usermod for this, and then gosu to drop to 
# the unprivileged user in entrypoint.sh
USER root

RUN echo "c.NotebookApp.default_url = '/lab'" >> /home/jovyan/.jupyter/jupyter_notebook_config.py

# silence warnings about descarteslabs credentials, they are provided via environment variables
RUN mkdir /home/jovyan/.descarteslabs
RUN echo "{}" >> /home/jovyan/.descarteslabs/token_info.json
# in case the descarteslabs package ever writes to this directory
RUN chown -R jovyan /home/jovyan/.descarteslabs

# we want to be able to update the repository inside a running container,
# but don't want to copy everything including local history.
# So we clone it from upstream. We could use `--depth` to reduce image
# size, but for now we won't so we can switch to other branches more easily
RUN git clone --recursive https://gitlab.com/kimetrica/darpa/darpa.git /usr/src/app

WORKDIR /usr/src/app
# start out with copy of example luigi file
RUN cp -v luigi.cfg.example luigi.cfg
RUN chown -R jovyan /usr/src/app

ENV PYTHONPATH=/usr/src/app

ENTRYPOINT ["tini", "-g", "--", "docker/kimetrica-darpa-models/entrypoint.sh"]
# # when setting an entrypoint, apparently the CMD gets reset?!
CMD ["start-notebook.sh"]
