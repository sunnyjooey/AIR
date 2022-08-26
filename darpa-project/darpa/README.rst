DARPA
=====


:License: MIT


Project setup
-------------

Setting up a development environment
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The setup is using ``docker-compose``, so after cloning the repository you should be able to build the Docker image by running ``docker-compose build``.

Running the container requires providing an ``.env`` file, that sets the necessary environment variables. You can just create that file by copying the provided example, e.g. ``cp -v env.example .env``.

Containers started via ``docker-compose`` will automatically mount the repository into the container, so you can just edit files inside the repository and the changes will immediately be reflected inside the container.


Authentication credentials
^^^^^^^^^^^^^^^^^^^^^^^^^^

A number of credentials are required to be able to run various models. Remember to update the ``.env`` file to include credentials generated.

Register for a ckan accout `here <https://data.kimetrica.com/user/register>`_. Ckan is used to store model input and output data.
Apply for a KDW accout `here <https://kdw.kimetrica.com/en/>`_ . KDW is used to store model input and output data.
Register for earth data account `here <https://urs.earthdata.nasa.gov/users/new>`_ . Earth data account is required to run hydrology model.
Sign up `here <https://iam.descarteslabs.com/>`_ for descartes lab account and follow `this <https://docs.descarteslabs.com/authentication.html>`_ instructions to generate ``DESCARTESLABS_CLIENT_ID`` and ``DESCARTESLABS_REFRESH_TOKEN``.
To save model outputs in s3, provide the values of ``AWS_ACCESS_KEY_ID`` and ``AWS_SECRET_ACCESS_KEY`` in the .env file.


Upgrading from controller setup
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Previously, the setup was based on ``docker[-compose]`` files from the ``kiluigi`` submodule. This has been removed, and ``kiluigi`` is installed as a Python package instead (in ``base.pip``).
The ``darpa`` repository now comes with its own ``docker[-compose]`` files, and there is only one container and not several containers for running things like Celery and a Django based API.

When switching to the single Docker image setup, make sure to adjust your copy of the ``.env`` file. The following line has to be removed or adjusted (compare with ``env.example``):

  COMPOSE_FILE=kiluigi/docker-compose.yml:kiluigi/docker-compose.override.yml:docker-compose.darpa.yml

Other than that the ``.env`` file should work for the single container setup as well, but compare your copy of ``.env`` to ``env.example`` to be sure.


Running Docker containers
^^^^^^^^^^^^^^^^^^^^^^^^^

This setup uses ``docker-compose``, to pass variables, mounts and other parameters to Docker.
To start a Jupyter notebook server with the repository mounted into the container for development, just execute ``docker-compose up``.
This will print the URL for accessing the notebook server to the console, e.g. ``http://127.0.0.1:8888/?token=20d967dde221356343a821fa8157ec48cb050d25b9717307``.

If docker-compose stops with an error message like this:

.. code-block::

  Attaching to darpa_kimetrica-darpa-models_1
  kimetrica-darpa-models_1  | Using usermod to set UID to
  kimetrica-darpa-models_1  | usermod: invalid user ID 'jovyan'
  darpa_kimetrica-darpa-models_1 exited with code 3

This means that the UID variable is not set in your environment. In this case, please run ``export UID`` once in your shell, or prepend each command with that variable, e.g. 

  UID=${UID} docker-compose up

For an explanation why the UID variable is required, please refer to `Technical details` below.

You can also run ``docker-compose up -d``. The optional ``-d`` switch will run the container in the background, without ``-d`` the output will be visible in your shell.


Running model pipelines on the command line
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

One option to run model pipelines is to run them inside a Notebook by prepending ``!`` to the command or opening a terminal instead of a Notebook in Jupyter lab. You can also attach to a running container to start a shell and execute models on the command line:

  docker-compose exec kimetrica-darpa-models bash

Please note that this will start the ``bash`` process inside the container as user ``root``. This can lead to problems with files that aren't accessible by unprivileged users, so it's better to attach to the running container as unprivileged user:

  docker-compose exec -u $UID kimetrica-darpa-models bash

``-u $UID`` will use the same UID (user ID) here as is used by Docker to run the main Notebook server process, so files created by either process are accessible by the regular user.

Another way is to start a fresh one-off container instead of attaching to an existing one.
For this, run 

  docker-compose run --rm kimetrica-darpa-models bash

Please note, that the `--rm` switch means that the container (including its ephemeral file system) will be deleted once the main process exits, so it's best suited for one-off tasks. You can omit the ``--rm`` switch and also give the one-off container a name, but it's unwieldy to work with one-off containers in that way, because you will have to restart and attach to the container later. So if a persistent file system is desired, it's best to work with Docker volumes or folders mounted into the containers from your operating system, or to attach to a long-running container (started earlier with ``docker-compose up [-d]``). Files in the long-running container will still be lost once the container is deleted, for something that really needs to persist mounts or Docker volumes are the best option.


Mounting folders into the container
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

You might want to mount additional folders into the container, to work on existing notebooks or work on Python libraries.
For this purpose, there is a template file that sets up additional mounts, called ``docker-compose.mounts.yml.example``. Create a copy of this file to adjust it:

  cp -v docker-compose.mounts.yml.example docker-compose.mounts.yml

You also have to activate this file, by uncommenting this line in your ``.env`` file:

  # COMPOSE_FILE=docker-compose.yml:docker-compose.override.yml:docker-compose.mounts.yml

This will include your copy of the ``docker-compose.mounts.yml`` file when running docker-compose.

The contents of this file are:

.. code-block:: yaml

    services:
    # extending the kimetrica-darpa-models service definition to add
    # additional mounts for directories containing jupyter notebooks or
    # Python source code packages
    kimetrica-darpa-models:
        image: kimetrica-darpa-models
        environment:
        # also adding folder for python source code packages to PYTHONPATH
        # so we can use and develop them
        - PYTHONPATH=/usr/src/app:/usr/src/vendor
        volumes:
        - ${PWD}/../interventions/interventions:/usr/src/vendor/interventions
        - ${PWD}/../notebooks:/usr/src/app/notebooks

This would mount a Python source code folder called ``interventions`` into ``/usr/src/vendor``, add ``/usr/src/vendor`` to ``PYTHONPATH`` so packages in this directory override previously installed packages of the same name.
It would also mount a ``notebooks`` folder into ``/usr/src/app/notebooks``, so those notebooks can be edited inside the container.

This assumes that you have the ``darpa`` repository cloned into the same folder as ``interventions`` and ``notebooks``, e.g.

.. code-block::

  Projects
  ├── darpa
  ├── interventions
  └── notebooks

Otherwise you might have to adjust the locations in the ``volumes`` section.
``docker-compose.mounts.yml`` will be ignored by git so your local copy doesn't get overwritten by changes made by other developers.

Technical details
"""""""""""""""""
To be able to write to files mounted into the container, the user has to have the same UID as the user on the host system that the files belong to (or the file permissions would have to be set in a way that all users can write to the files). Normally, the user in the Jupyter notebook container (called ``jovyan``) has the UID ``1000``, which is usually the UID of the first unprivileged user that is created on a system.

However, on some systems your local user might have a different UID like ``1001``.
To enable such users to still write to the mounted files, the UID of the ``jovyan`` user is changed to the UID of the user of the host system on startup. This way both users will have the same UID, and the ``jovyan`` user can write to files that are owned by the user on the host system.

This happens by using an ``entrypoint.sh`` script that runs ``usermod`` to update the UID, and then uses ``gosu`` to execute the main process as unprivileged user.
For ``usermod`` to be able to work, it has to be started as ``root``, which means we cannot change to the ``jovyan`` user already in the ``Dockerfile``.

For this to work, the UID variable has to be set in your environment. Normally that should be the case. If it's not, like when the user was changed using the ``su`` command, you have to run ``export UID`` once in the terminal session, or prepend each command with 

  UID=${UID} 


Ensuring standard compliant source code files
---------------------------------------------

Kimetrica uses `black <https://github.com/ambv/black>`_ and `flake8 <http://flake8.pycqa.org/en/latest/>`_ to enforce correctly formatted and syntactically correct source code files.
Please install the pre-commit hook into your local git clone to make sure that all files are checked whether they are standard compliant before they can be submitted to the repository.

Hook installation instructions
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Just copy or link the ``pre-commit`` file into your ``.git/hooks`` folder, e.g.

  cp -v pre-commit .git/hooks/

To check your source code for compliance, you can either run ``flake8`` and ``black`` manually, or just execute the hook: ``./pre-commit``
