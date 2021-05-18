import logging
import os
import shutil
import threading
import time

import jsonpickle
import pandas
import yaml

from app.algo import combine, get_categories, encode_categorical


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

    def read_config(self):
        print(f"Read config file.", flush=True)
        with open(os.path.join(self.INPUT_DIR, "config.yml")) as f:
            config = yaml.load(f, Loader=yaml.FullLoader)["fc_one_hot_encoding"]
            self.input_filename = config["files"]["input_filename"]
            self.output_filename = config["files"]["output_filename"]
            self.sep = config["files"]["sep"]
        shutil.copyfile(os.path.join(self.INPUT_DIR, "config.yml"), os.path.join(self.OUTPUT_DIR, "config.yml"))

    def read_data(self):
        path = os.path.join(self.INPUT_DIR, self.input_filename)
        logging.info(f"Read data file at {path}")
        dataframe = pandas.read_csv(path, sep=self.sep)
        logging.debug(f"\n{dataframe}")
        return dataframe

    def encode_data(self):
        logging.info(f"Encode data")
        self.encoded_data = encode_categorical(self.data, self.aggregated_col_info)
        logging.debug(f"Column names:\t{self.encoded_data.columns}")

    def write_output(self, path):
        logging.info(f"Write data to {path}")
        self.encoded_data.to_csv(path, sep=self.sep, index=False)

    def app_flow(self):
        # This method contains a state machine for the participant and coordinator instance

        # === States ===
        state_initializing = 1
        state_read_input = 2
        state_summarize_columns = 3
        state_wait_for_aggregation = 4
        state_global_aggregate_col_info = 5
        state_encode_data = 6
        state_finish = 7

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
                    state = state_read_input
                print("[CLIENT] Initializing finished.", flush=True)

            if state == state_read_input:
                self.progress = "read input..."
                print("[CLIENT] Read input...", flush=True)
                # Read the config file
                self.read_config()
                # read input files
                self.data = self.read_data()
                state = state_summarize_columns
                print("[CLIENT] Read input finished.", flush=True)

            if state == state_summarize_columns:
                self.progress = "summarize columns..."
                print("[CLIENT] Summarize columns...", flush=True)

                # Compute local results
                columns_summary = get_categories(self.data)
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
                    self.aggregated_col_info = combine(data)
                    logging.debug(f"combined:\t{self.aggregated_col_info}")
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
