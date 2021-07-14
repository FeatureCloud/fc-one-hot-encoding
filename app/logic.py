import logging
import os
import shutil
import threading
import time
from typing import Optional, Dict, List

import jsonpickle
import pandas
import yaml

from app.algo import combine, get_categories, encode_categorical, drop_rows_with_introduced_na_values


class AppLogic:

    def __init__(self):
        # === Status of this app instance ===

        # Indicates whether there is data to share, if True make sure self.data_out is available
        self.status_available = False

        # Will stop execution when True
        self.status_finished = False

        # === Data ===
        self.data_incoming = []
        self.data_outgoing = None

        # === Parameters set during setup ===
        self.id = None
        self.coordinator = None
        self.clients = None

        # === Directories, input files always in INPUT_DIR. Write your output always in OUTPUT_DIR
        self.INPUT_DIR = "/mnt/input"
        self.OUTPUT_DIR = "/mnt/output"

        # === Variables from config.yml
        self.input_filename = None
        self.sep = None
        self.output_filename = None
        self.mode = None
        self.study_definition: Optional[Dict[str, List[str]]] = None

        # === Internals ===
        self.thread = None
        self.iteration = 0
        self.progress = "not started yet"
        self.data = None
        self.encoded_data = None
        self.aggregated_col_info = None

    def handle_setup(self, client_id, master, clients):
        # This method is called once upon startup and contains information about the execution context of this instance
        self.id = client_id
        self.coordinator = master
        self.clients = clients
        print(f"Received setup: {self.id} {self.coordinator} {self.clients}", flush=True)

        self.thread = threading.Thread(target=self.app_flow)
        self.thread.start()

    def handle_incoming(self, data):
        # This method is called when new data arrives
        print("Process incoming data....", flush=True)
        self.data_incoming.append(data.read())

    def handle_outgoing(self):
        print("Process outgoing data...", flush=True)
        # This method is called when data is requested
        self.status_available = False
        return self.data_outgoing

    def parse_study_definition(self, config):
        directive = "categorical_variables"
        definition = config.get(directive)
        logging.debug(f"definition:\t{definition}")
        if definition is None:
            raise ValueError(f"When mode is set to {self.mode!r} the config file of the coordinator "
                             f"must define a {directive!r} directive.")

        # check if we have the structure Dict[str, List[str]]
        definition_structure_help_text = f"The {directive!r} directive must be a mapping of column names to a list.\n" \
                                         f"E.g.:\n" \
                                         f"fc_one_hot_encoding:\n" \
                                         f"  {directive}:\n" \
                                         f"    Celltype: ['large', 'adeno', 'smallcell', 'squamous']\n" \
                                         f"    Prior_therapy: ['no', 'yes']\n" \
                                         f"    Treatment: ['test', 'standard']"
        if type(definition) is not dict:
            raise ValueError(definition_structure_help_text)
        for key, value in definition.items():
            if type(key) is not str:
                raise ValueError(definition_structure_help_text)
            if type(value) is not list:
                raise ValueError(definition_structure_help_text)

        self.study_definition = definition
        logging.debug(f"study_definition:\t{self.study_definition}")

    def read_config(self):
        logging.debug(f"Read config file.")
        with open(os.path.join(self.INPUT_DIR, "config.yml")) as f:
            config = yaml.load(f, Loader=yaml.FullLoader)["fc_one_hot_encoding"]
            self.input_filename = config["files"]["input_filename"]
            self.output_filename = config["files"]["output_filename"]
            self.sep = config["files"]["sep"]

            self.mode = config["mode"]
            if self.mode not in ["auto", "predefined"]:
                raise ValueError("Unknown mode")
            logging.debug(f"Mode: {self.mode}")

            if self.mode == "predefined":
                if self.coordinator:
                    self.parse_study_definition(config)

        logging.debug("Copy config file")
        shutil.copyfile(os.path.join(self.INPUT_DIR, "config.yml"), os.path.join(self.OUTPUT_DIR, "config.yml"))

    def read_data(self):
        path = os.path.join(self.INPUT_DIR, self.input_filename)
        logging.info(f"Read data file at {path}")
        dataframe = pandas.read_csv(path, sep=self.sep)
        logging.debug(f"\n{dataframe}")
        return dataframe

    def encode_data(self):
        logging.info(f"Encode data")
        encoded_df = encode_categorical(self.data, self.aggregated_col_info)
        self.encoded_data = drop_rows_with_introduced_na_values(self.data, encoded_df)
        logging.debug(f"Column names:\t{self.encoded_data.columns}")

    def write_output(self, path):
        logging.info(f"Write data to {path}")
        self.encoded_data.to_csv(path, sep=self.sep, index=False)

    @staticmethod
    def check_agree(data: List[str]):
        return len(set(data)) == 1

    def app_flow(self):
        # This method contains a state machine for the participant and coordinator instance

        # === States ===
        state_initializing = 1
        state_read_config = 2
        state_send_mode = 3
        state_global_check_mode_agreement = 4
        state_wait_for_mode_agreement = 5
        state_read_input = 6
        state_summarize_columns = 7
        state_wait_for_aggregation = 8
        state_global_aggregate_col_info = 9
        state_encode_data = 10
        state_finish = 11

        # Initial state
        state = state_initializing
        while True:

            if state == state_initializing:
                self.progress = "initializing..."
                print("[CLIENT] Initializing...", flush=True)
                if self.id is not None:  # Test is setup has happened already
                    if self.coordinator:
                        print("I am the coordinator.", flush=True)
                    else:
                        print("I am a participating client.", flush=True)
                    state = state_read_config
                print("[CLIENT] Initializing finished.", flush=True)

            if state == state_read_config:
                self.progress = "read config..."
                print("[CLIENT] Read config...", flush=True)
                # Read the config file
                self.read_config()
                state = state_send_mode
                print("[CLIENT] Read config finished.", flush=True)

            if state == state_send_mode:
                self.progress = "send mode..."
                print("[CLIENT] Send mode...", flush=True)
                mode = self.mode
                logging.debug(f"mode:\t{mode}")
                # Encode local results to send it to coordinator
                data_to_send = jsonpickle.encode(mode)

                if self.coordinator:
                    # if the client is the coordinator: add the local results directly to the data_incoming array
                    self.data_incoming.append(data_to_send)
                    # go to state where the coordinator is waiting for the local results and aggregates them
                    state = state_global_check_mode_agreement
                else:
                    # if the client is not the coordinator: set data_outgoing and set status_available to true
                    self.data_outgoing = data_to_send
                    self.status_available = True
                    # go to state where the client is waiting for the aggregated results
                    state = state_wait_for_mode_agreement
                    print('[CLIENT] Send mode to coordinator', flush=True)
                print("[CLIENT] Send mode finished.", flush=True)

            # GLOBAL AGGREGATION
            if state == state_global_check_mode_agreement:
                self.progress = "aggregate mode information..."
                print("[COORDINATOR] Aggregate mode information...", flush=True)
                if len(self.data_incoming) == len(self.clients):
                    print("[COORDINATOR] Received mode of all participants.", flush=True)
                    print("[COORDINATOR] Checking agreement on mode...", flush=True)
                    # Decode received data of each client
                    data = [jsonpickle.decode(client_data) for client_data in self.data_incoming]
                    # Empty the incoming data (important for multiple iterations)
                    self.data_incoming = []
                    # Perform global aggregation
                    agreement = self.check_agree(data)
                    logging.debug(f"agreement:\t{agreement}")
                    # Encode aggregated results for broadcasting
                    data_to_broadcast = jsonpickle.encode(agreement)

                    # Fill data_outgoing
                    self.data_outgoing = data_to_broadcast
                    # Set available to True such that the data will be broadcasted
                    self.status_available = True
                    # go to state where the client is waiting for the aggregated results

                    if not agreement:
                        state = state_finish
                    else:
                        state = state_read_input
                    print("[COORDINATOR] Checking agreement on mode finished.", flush=True)
                    time.sleep(10)
                else:
                    print(
                        f"[COORDINATOR] Mode information of {str(len(self.clients) - len(self.data_incoming))} client(s) still "
                        f"missing...)", flush=True)

            if state == state_wait_for_mode_agreement:
                self.progress = "wait for mode agreement information..."
                print("[CLIENT] Wait for mode agreement information...", flush=True)
                # Wait until received broadcast data from coordinator
                if len(self.data_incoming) > 0:
                    print("[CLIENT] Process aggregated result from coordinator...", flush=True)
                    # Decode broadcasted data
                    agreement = jsonpickle.decode(self.data_incoming[0])
                    logging.debug(f"agreement:\t{agreement}")
                    logging.debug(f"mode:\t{self.mode}")
                    # Empty incoming data
                    self.data_incoming = []

                    if not agreement:
                        raise ValueError("Participants do not agree on mode")

                    # Go to nex state (finish)
                    state = state_read_input
                    print("[CLIENT] Mode agreement finished.", flush=True)

            if state == state_read_input:
                self.progress = "read input..."
                print("[CLIENT] Read input...", flush=True)
                # read input files
                self.data = self.read_data()
                state = state_summarize_columns
                print("[CLIENT] Read input finished.", flush=True)

            if state == state_summarize_columns:
                if self.mode == "auto":
                    self.progress = "summarize columns..."
                    print("[CLIENT] Summarize columns...", flush=True)

                    # Compute local results
                    columns_summary = get_categories(self.data)
                else:
                    if self.coordinator:
                        columns_summary = self.study_definition
                    else:
                        columns_summary = None  # send None when predefined mode and node is not coordinator

                logging.debug(f"columns_summary:\t{columns_summary}")
                # Encode local results to send it to coordinator
                data_to_send = jsonpickle.encode(columns_summary)

                if self.coordinator:
                    # if the client is the coordinator: add the local results directly to the data_incoming array
                    self.data_incoming.append(data_to_send)
                    # go to state where the coordinator is waiting for the local results and aggregates them
                    state = state_global_aggregate_col_info
                else:
                    # if the client is not the coordinator: set data_outgoing and set status_available to true
                    self.data_outgoing = data_to_send
                    self.status_available = True
                    # go to state where the client is waiting for the aggregated results
                    state = state_wait_for_aggregation
                    print('[CLIENT] Send data to coordinator', flush=True)
                print("[CLIENT] Compute local results finished.", flush=True)

            # GLOBAL AGGREGATION
            if state == state_global_aggregate_col_info:
                self.progress = "aggregate column information..."
                print("[COORDINATOR] Aggregate column information...", flush=True)
                if len(self.data_incoming) == len(self.clients):
                    print("[COORDINATOR] Received data of all participants.", flush=True)
                    print("[COORDINATOR] Merging results...", flush=True)
                    # Decode received data of each client
                    data = [jsonpickle.decode(client_data) for client_data in self.data_incoming]
                    # Empty the incoming data (important for multiple iterations)
                    self.data_incoming = []
                    # Perform global aggregation
                    if self.mode == "auto":
                        self.aggregated_col_info = combine(data)
                        logging.debug(f"combined:\t{self.aggregated_col_info}")
                    else:
                        # wait for other nodes to send something but ignore and return predefined
                        self.aggregated_col_info = self.study_definition
                    # Encode aggregated results for broadcasting
                    data_to_broadcast = jsonpickle.encode(self.aggregated_col_info)
                    # Fill data_outgoing
                    self.data_outgoing = data_to_broadcast
                    # Set available to True such that the data will be broadcasted
                    self.status_available = True
                    state = state_encode_data
                    print("[COORDINATOR] Global aggregation finished.", flush=True)
                else:
                    print(
                        f"[COORDINATOR] Data of {str(len(self.clients) - len(self.data_incoming))} client(s) still "
                        f"missing...)", flush=True)

            if state == state_wait_for_aggregation:
                self.progress = "wait for aggregated results..."
                print("[CLIENT] Wait for aggregated results from coordinator...", flush=True)
                # Wait until received broadcast data from coordinator
                if len(self.data_incoming) > 0:
                    print("[CLIENT] Process aggregated result from coordinator...", flush=True)
                    # Decode broadcasted data
                    self.aggregated_col_info = jsonpickle.decode(self.data_incoming[0])
                    logging.debug(self.aggregated_col_info)
                    logging.debug(encode_categorical(self.data, self.aggregated_col_info))
                    # Empty incoming data
                    self.data_incoming = []
                    # Go to nex state (finish)
                    state = state_encode_data
                    print("[CLIENT] Processing aggregated results finished.", flush=True)

            if state == state_encode_data:
                self.progress = "encode data..."
                print("[CLIENT] Encode data...", flush=True)
                self.encode_data()
                state = state_finish
                print("[CLIENT] Encode data finished.", flush=True)

            if state == state_finish:
                self.progress = "finishing..."
                print("[CLIENT] FINISHING", flush=True)

                # Write results
                logging.info(f"Writing final results...")
                output_path = os.path.join(self.OUTPUT_DIR, self.output_filename)
                self.write_output(output_path)

                # Wait some seconds to make sure all clients have written the results. This will be fixed soon.
                if self.coordinator:
                    time.sleep(5)

                # Set finished flag to True, which ends the computation
                self.status_finished = True
                self.progress = "finished."
                break

            time.sleep(1)


logic = AppLogic()
