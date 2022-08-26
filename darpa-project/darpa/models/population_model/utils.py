# -*- coding: utf-8 -*-

"""
models.population_model.utils
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Various utility functions for population model
"""


def estimate_current_population(
    previous_population, crude_birth_rate, crude_death_rate, inmigration, outmigration
):
    """
    Estimates "current" population based on the latest census data,
    a Crude Birth Rate (CBR), a Crude Death Rate (CDR), and
    In/Out Migration counts.
    """
    return (
        previous_population
        + (previous_population * crude_birth_rate)
        - (previous_population * crude_death_rate)
        + inmigration
        - outmigration
    )
