class Config(object):

    fig_dir = "/Users/AlisonCampion/Documents/git_repositories/darpa_economic_model/cache/intermediate_results"
    config_ert_cont = {}
    config_ert_class = {}
    CV = {}
    KF = {}

    # -------------------
    # Print debug logging
    # -------------------
    log = True

    # -------------------------------------
    # Options specific for Cross-Validation
    # -------------------------------------
    CV["n_jobs"] = 1
    CV["verbose"] = 0
    KF["n_folds"] = 5
    KF["random_state"] = True

    ki_color = (28.0 / 255.0, 180.0 / 255.0, 229.0 / 255.0)

    # ----------------------------------
    # Options specific for ERT Regressor
    # ----------------------------------
    config_ert_cont["n_estimators"] = 2500
    config_ert_cont["criterion"] = "mse"
    config_ert_cont["max_depth"] = None
    config_ert_cont["min_samples_split"] = 2
    config_ert_cont["min_samples_leaf"] = 1
    config_ert_cont["min_weight_fraction_leaf"] = 0.0
    config_ert_cont["random_state"] = None
    config_ert_cont["verbose"] = 0
    config_ert_cont["max_features"] = "auto"
    config_ert_cont["max_leaf_nodes"] = None
    config_ert_cont["oob_score"] = True
    config_ert_cont["bootstrap"] = True
    config_ert_cont["warm_start"] = False

    # -----------------------------------
    # Options specific for ERT Classifier
    # -----------------------------------
    config_ert_class["n_estimators"] = 2500
    config_ert_class["criterion"] = "gini"
    config_ert_class["max_depth"] = None
    config_ert_class["min_samples_split"] = 2
    config_ert_class["min_samples_leaf"] = 1
    config_ert_class["min_weight_fraction_leaf"] = 0.0
    config_ert_class["random_state"] = None
    config_ert_class["verbose"] = 0
    config_ert_class["max_features"] = "auto"
    config_ert_class["max_leaf_nodes"] = None
    config_ert_class["oob_score"] = True
    config_ert_class["bootstrap"] = True
    config_ert_class["warm_start"] = False

    param_grid = {
        "n_estimators": [500, 1000, 2000, 5000, 7000],
        "max_features": ["auto", "sqrt", "log2"],
        "max_depth": [None, 1, 5, 10, 50, 100, 200, 500],
        "min_samples_split": [2, 5, 10, 20, 50, 100],
        "min_samples_leaf": [1, 5, 10, 20, 50, 100],
        "min_impurity_decrease": [0.0, 0.1, 0.2, 0.4, 0.6, 0.8, 1, 5],
        "max_leaf_nodes": [None, 5, 10, 30, 50, 100],
        # "oob_score": [True, False],
    }
