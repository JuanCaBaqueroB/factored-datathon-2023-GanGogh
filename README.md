# factored-datathon-2023-GanGogh

├── README.md                 # Project overview and documentation
├── requirements.txt          # Dependencies to install for the project
├── setup.cfg                 # Configurations of the execution environment
├── examples                  # Examples about how to use the project
│   └── notebooks             # Jupyter notebooks for data exploration, analysis, experiment
├── src                       # Source code for data processing, modeling, training, eval
│   ├── load_data_step        # Code to load input data
│   ├── train_step            # Code to train a model
│   ├── predict_step          # Code to predict with a previously existing model
│   ├── preprocess_step       # Code to preprocess data before going into the model
│   ├── models                # Machine learning models to use
│   ├── evaluate_step         # Code to compute evaluation metrics
│   ├── ...
│   └── common_utils          # Functions and constants shared across multiple modules
├── tests                     # Code to perform unit testing
├── docs                      # Documentation, presentations, reports, etc
└── terraform                 # Code to provision infrastructure with Terraform
