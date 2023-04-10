import os
import shutil
import threading
import time
from typing import Optional, Dict, List

import jsonpickle
import pandas
import yaml

from FeatureCloud.app.engine.app import AppState, app_state, Role
from algo import combine, get_categories, encode_categorical, drop_rows_with_introduced_na_values

@app_state('initial', Role.BOTH)
class InitialState(AppState):
    """
    Initialize client.
    """

    def register(self):
        self.register_transition('read config', Role.BOTH)
        
    def run(self) -> str or None:
        self.log("[CLIENT] Initializing")
        if self.id is not None:  # Test if setup has happened already
            self.log(f"[CLIENT] Coordinator {self.is_coordinator}")
        
        return 'read config'


@app_state('read config', Role.BOTH)
class ReadConfigState(AppState):
    """
    Read config file.
    """

    def register(self):
        self.register_transition('send mode', Role.BOTH)
        
    def run(self) -> str or None:
        self.log("[CLIENT] Read input and config")
        self.read_config()
        return 'send mode'
        
    def read_config(self):
        self.log(f"Read config file.")
        self.store('INPUT_DIR', "/mnt/input")
        self.store('OUTPUT_DIR', "/mnt/output")
        with open(os.path.join(self.load('INPUT_DIR'), "config.yml")) as f:
            config = yaml.load(f, Loader=yaml.FullLoader)["fc_one_hot_encoding"]
            self.store('input_filename', config["files"]["input_filename"])
            self.store('output_filename', config["files"]["output_filename"])
            self.store('sep', config["files"]["sep"])

            self.store('mode', config["mode"])
            if self.load('mode') not in ["auto", "predefined"]:
                raise ValueError("Unknown mode")
            self.log(f"Mode: {self.load('mode')}")

            if self.load('mode') == "predefined":
                if self.is_coordinator:
                    self.parse_study_definition(config)

        self.log("Copy config file")
        shutil.copyfile(os.path.join(self.load('INPUT_DIR'), "config.yml"), os.path.join(self.load('OUTPUT_DIR'), "config.yml"))

    def parse_study_definition(self, config):
        directive = "categorical_variables"
        definition = config.get(directive)
        self.log(f"definition:\t{definition}")
        if definition is None:
            raise ValueError(f"When mode is set to {self.load('mode')!r} the config file of the coordinator "
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
        
        self.store('study_definition', definition)
        self.log(f"study_definition:\t{self.load('study_definition')}")


@app_state('send mode', Role.BOTH)
class SendModeState(AppState):
    """
    Send the local results to the coordinator.
    """

    def register(self):
        self.register_transition('global check mode agreement', Role.COORDINATOR)
        self.register_transition('wait for mode agreement', Role.PARTICIPANT)
        
    def run(self) -> str or None:
        self.log("[CLIENT] Send mode...")
        mode = self.load('mode')
        self.log(f"mode:\t{mode}")
        # Encode local results to send it to coordinator
        data_to_send = jsonpickle.encode(mode)
        self.send_data_to_coordinator(data_to_send)
        self.log('[CLIENT] Send mode to coordinator')
        if self.is_coordinator:
            return 'global check mode agreement'
        else:
            return 'wait for mode agreement'

# GLOBAL AGGREGATION
@app_state('global check mode agreement', Role.COORDINATOR)
class GlobalCheckModeAgreementState(AppState):
    """
    The coordinator receives the local computation data from each client and aggregates it.
    The coordinator broadcasts the global computation data to the clients.
    """

    def register(self):
        self.register_transition('finish', Role.COORDINATOR)
        self.register_transition('read input', Role.COORDINATOR)
        
    def run(self) -> str or None:
        self.log("[COORDINATOR] Aggregate mode information...")
        data_incoming = self.gather_data()
        self.log("[COORDINATOR] Received mode of all participants.")
        self.log("[COORDINATOR] Checking agreement on mode...")
        # Decode received data of each client
        data = [jsonpickle.decode(client_data) for client_data in data_incoming]
        # Perform global aggregation
        agreement = self.check_agree(data)
        self.log(f"agreement:\t{agreement}")
        # Encode aggregated results for broadcasting
        data_to_broadcast = jsonpickle.encode(agreement)
        self.broadcast_data(data_to_broadcast, send_to_self=False)

        # go to state where the client is waiting for the aggregated results
        if not agreement:
            return 'finish'
        else:
            self.log("[COORDINATOR] Checking agreement on mode finished.")
            time.sleep(10)
            return 'read input'
            
    @staticmethod
    def check_agree(data: List[str]):
        return len(set(data)) == 1


@app_state('wait for mode agreement', Role.PARTICIPANT)
class WaitForModeAgreementState(AppState):
    """
    The participant waits until it receives the aggregation data from the coordinator.
    """

    def register(self):
        self.register_transition('read input', Role.PARTICIPANT)
        
    def run(self) -> str or None:
        self.log("[CLIENT] Wait for mode agreement information...")
        # Wait until received broadcast data from coordinator
        data = self.await_data()
        self.log("[CLIENT] Process aggregated result from coordinator...")
        # Decode broadcasted data
        agreement = jsonpickle.decode(data)
        self.log(f"agreement:\t{agreement}")
        self.log(f"mode:\t{self.load('mode')}")

        if not agreement:
            raise ValueError("Participants do not agree on mode")

        # Go to nex state (finish)
        self.log("[CLIENT] Mode agreement finished.")
        return 'read input'
  
  
@app_state('read input', Role.BOTH)
class ReadInputState(AppState):
    """
    Read input data.
    """

    def register(self):
        self.register_transition('summarize columns', Role.BOTH)
        
    def run(self) -> str or None:
        self.log("[CLIENT] Read input...")
        # read input files
        data = self.read_data()
        self.store('data', data)
        self.log("[CLIENT] Read input finished.")
        return 'summarize columns'

    def read_data(self):
        path = os.path.join(self.load('INPUT_DIR'), self.load('input_filename'))
        self.log(f"Read data file at {path}")
        dataframe = pandas.read_csv(path, sep=self.load('sep'))
        self.log(f"\n{dataframe}")
        return dataframe
        

@app_state('summarize columns', Role.BOTH)
class SummarizeColumnsState(AppState):
    """
    Compute local results and send it to the coordinator.
    """

    def register(self):
        self.register_transition('global aggregate col info', Role.COORDINATOR)
        self.register_transition('wait for aggregation', Role.PARTICIPANT)
        
    def run(self) -> str or None:
        if self.load('mode') == "auto":
            self.log("[CLIENT] Summarize columns...")
            # Compute local results
            columns_summary = get_categories(self.load('data'))
        else:
            if self.is_coordinator:
                columns_summary = self.load('study_definition')
            else:
                columns_summary = None  # send None when predefined mode and node is not coordinator

        self.log(f"columns_summary:\t{columns_summary}")
        # Encode local results to send it to coordinator
        data_to_send = jsonpickle.encode(columns_summary)
        self.send_data_to_coordinator(data_to_send)
        
        if self.is_coordinator:
            self.log("[CLIENT] Compute local results finished.")
            # go to state where the coordinator is waiting for the local results and aggregates them
            return 'global aggregate col info'
        else:
            self.log('[CLIENT] Send data to coordinator')
            self.log("[CLIENT] Compute local results finished.")
            # go to state where the client is waiting for the aggregated results
            return 'wait for aggregation'
   
# GLOBAL AGGREGATION
@app_state('global aggregate col info', Role.COORDINATOR)
class GlobalAggregateColInfoState(AppState):
    """
    Aggregate column information.
    """

    def register(self):
        self.register_transition('encode data', Role.COORDINATOR)
        
    def run(self) -> str or None:
        self.log("[COORDINATOR] Aggregate column information...")
        data_incoming = self.gather_data()
        self.log("[COORDINATOR] Received data of all participants.")
        self.log("[COORDINATOR] Merging results...")
        # Decode received data of each client
        data = [jsonpickle.decode(client_data) for client_data in data_incoming]
        # Perform global aggregation
        if self.load('mode') == "auto":
            aggregated_col_info = combine(data)
            self.store('aggregated_col_info', aggregated_col_info)
            self.log(f"combined:\t{self.load('aggregated_col_info')}")
        else:
            # wait for other nodes to send something but ignore and return predefined
            aggregated_col_info = self.load('study_definition')
            self.store('aggregated_col_info', aggregated_col_info)
        # Encode aggregated results for broadcasting
        data_to_broadcast = jsonpickle.encode(self.load('aggregated_col_info'))
        self.broadcast_data(data_to_broadcast, send_to_self=False)
        self.log("[COORDINATOR] Global aggregation finished.")
        return 'encode data'
        
        
@app_state('wait for aggregation', Role.PARTICIPANT)
class WaitForAggregationState(AppState):
    """
    Wait for aggregated results from coordinator..
    """

    def register(self):
        self.register_transition('encode data', Role.PARTICIPANT)
        
    def run(self) -> str or None:
        self.log("[CLIENT] Wait for aggregated results from coordinator...")
        # Wait until received broadcast data from coordinator
        data = self.await_data()
        self.log("[CLIENT] Process aggregated result from coordinator...")
        # Decode broadcasted data
        aggregated_col_info = jsonpickle.decode(data)
        self.store('aggregated_col_info', aggregated_col_info)
        self.log(self.load('aggregated_col_info'))
        self.log(encode_categorical(self.load('data'), self.load('aggregated_col_info')))
        self.log("[CLIENT] Processing aggregated results finished.")
        # Go to nex state (finish)
        return 'encode data'


@app_state('encode data', Role.BOTH)
class EncodeDataState(AppState):
    """
    Encode the data.
    """

    def register(self):
        self.register_transition('finish', Role.BOTH)
        
    def run(self) -> str or None:
        self.log("[CLIENT] Encode data...")
        self.encode_data()
        self.log("[CLIENT] Encode data finished.")
        return 'finish'
    
    def encode_data(self):
        self.log(f"Encode data")
        encoded_df = encode_categorical(self.load('data'), self.load('aggregated_col_info'))
        encoded_data = drop_rows_with_introduced_na_values(self.load('data'), encoded_df)
        self.store('encoded_data', encoded_data)
        self.log(f"Column names:\t{self.load('encoded_data.columns')}")
 
 
@app_state('finish', Role.BOTH)
class FinishState(AppState):
    """
    Write final results.
    """

    def register(self):
        self.register_transition('terminal', Role.BOTH)
        
    def run(self) -> str or None:
        self.log("[CLIENT] FINISHING")

        # Write results
        self.log(f"Writing final results...")
        output_path = os.path.join(self.load('OUTPUT_DIR'), self.load('output_filename'))
        self.write_output(output_path)
        self.send_data_to_coordinator('DONE')
        # Wait some seconds to make sure all clients have written the results.
        if self.is_coordinator:
            self.gather_data()

        return 'terminal'
        
    def write_output(self, path):
        self.log(f"Write data to {path}")
        self.load('encoded_data').to_csv(path, sep=self.load('sep'), index=False)
