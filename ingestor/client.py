import os
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta
from enum import StrEnum

import scicat_sdk_py as sc
from scicat_sdk_py import (CreateDatasetDto, CreateOrigDatablockDto,
                           CreateProposalDto, DataFile, CreateSampleDto)


class AuthStrategy(StrEnum):
    FUNCTIONAL = "login"
    LDAP = "ldap"


class SciCatClient:
    def __init__(self, config, log):
        self.log = log
        self.session = None
        self.config = config
        self.clientconfig = sc.Configuration(host=config.url)
        self.expires = datetime.now().timestamp()

    def login(self, user, password):
        with sc.ApiClient(self.clientconfig) as api_client:
            # Create an instance of the API class
            auth = sc.AuthApi(api_client)
            credentials_dto = sc.CredentialsDto(
                username=self.config.user,
                password=self.config.pw,
            )
            try:
                if self.config.auth_strat == AuthStrategy.FUNCTIONAL:
                    response = auth.auth_controller_login(credentials_dto)
                else:
                    response = auth.auth_controller_ldap_login(credentials_dto)
            except Exception as e:
                # TODO
                self.log.debug("Exception when calling AuthApi->auth_controller_ldap_login: %s\n" % e)
        self.clientconfig.access_token = response.access_token
        self.expires = datetime.fromisoformat(response.created) + timedelta(seconds=response.ttl)

    def ensure_logged_in(f):
        def inner(self, *args, **kwargs):
            if self.expires.timestamp() < datetime.now().timestamp():
                # we have to refresh the token
                self.login(self.config.user, self.config.pw)
            return f(self, *args, **kwargs)
        return inner

    @ensure_logged_in
    def create_dataset(self, dataset):
        with sc.ApiClient(self.clientconfig) as api_client:
            # Create an instance of the API class
            api = sc.DatasetsApi(api_client)
            try:
                create_req = sc.DatasetsControllerCreateRequest(dataset)
                response = api.datasets_controller_create(create_req)
            except Exception as e:
                # TODO
                self.log.info("Exception when calling DatasetsApi->datasets_controller_create: %s\n" % e)
                return None
        return response

    @ensure_logged_in
    def get_dataset_by_pid(self, pid):
        with sc.ApiClient(self.clientconfig) as api_client:
            # Create an instance of the API class
            api = sc.DatasetsApi(api_client)
            try:
                response = api.datasets_controller_find_by_id(pid)
            except Exception as e:
                # TODO
                self.log.debug("Exception when calling DatasetsApi->datasets_controller_find_by_id: %s\n" % e)
                return None
        return response

    @ensure_logged_in
    def get_origdatablocks_for_dataset(self, pid):
        with sc.ApiClient(self.clientconfig) as api_client:
            # Create an instance of the API class
            api = sc.DatasetsApi(api_client)
            try:
                response = api.datasets_controller_find_all_orig_datablocks(pid)
            except Exception as e:
                # TODO
                self.log.debug("Exception when calling DatasetsApi->datasets_controller_find_all_orig_datablocks: %s\n" % e)
                return None
        return response

    @ensure_logged_in
    def create_origdatablock(self, datablock):
        with sc.ApiClient(self.clientconfig) as api_client:
            # Create an instance of the API class
            api = sc.OrigdatablocksApi(api_client)
            try:
                response = api.orig_datablocks_controller_create(datablock)
            except Exception as e:
                # TODO
                self.log.info("Exception when calling OrigdatablocksApi->orig_datablocks_controller_create: %s\n" % e)
                return None
        return response

    @ensure_logged_in
    def get_or_create_proposal(self, proposal):
        present = self.get_proposal_by_pid(proposal.proposal_id)
        if present is not None:
            return present
        return self.create_proposal(proposal)

    @ensure_logged_in
    def create_proposal(self, proposal):
        with sc.ApiClient(self.clientconfig) as api_client:
            # Create an instance of the API class
            api = sc.ProposalsApi(api_client)
            try:
                response = api.proposals_controller_create(proposal)
            except Exception as e:
                # TODO
                self.log.info("Exception when calling ProposalsApi->proposals_controller_create: %s\n" % e)
                return None
        return response

    @ensure_logged_in
    def get_proposal_by_pid(self, pid):
        with sc.ApiClient(self.clientconfig) as api_client:
            # Create an instance of the API class
            api = sc.ProposalsApi(api_client)
            try:
                response = api.proposals_controller_find_by_id(pid)
            except Exception as e:
                # TODO
                self.log.debug("Exception when calling ProposalsApi->proposals_controller_find_by_id: %s\n" % e)
                return None
        return response

    @ensure_logged_in
    def get_instrument_by_pid(self, pid):
        with sc.ApiClient(self.clientconfig) as api_client:
            # Create an instance of the API class
            api = sc.InstrumentsApi(api_client)
            try:
                response = api.instruments_controller_find_by_id(pid)
            except Exception as e:
                # TODO
                self.log.debug("Exception when calling InstrumentsApi->instruments_controller_find_by_id: %s\n" % e)
                return None
        return response

    @ensure_logged_in
    def search_instruments(self, filter=None):
        with sc.ApiClient(self.clientconfig) as api_client:
            # Create an instance of the API class
            api = sc.InstrumentsApi(api_client)
            try:
                response = api.instruments_controller_find_all(filter)
            except Exception as e:
                # TODO
                self.log.info("Exception when calling InstrumentsApi->instruments_controller_find_all: %s\n" % e)
                return None
        return response

    @ensure_logged_in
    def instrument_by_unique_name(self, uname):
        return self.search_instruments(filter={"where": {"uniqueName": uname}})

    @ensure_logged_in
    def create_sample(self, sample):
        with sc.ApiClient(self.clientconfig) as api_client:
            # Create an instance of the API class
            api = sc.SamplesApi(api_client)
            try:
                response = api.samples_controller_create(sample)
            except Exception as e:
                # TODO
                self.log.info("Exception when calling Samples->samples_controller_create: %s\n" % e)
                return None
        return response

    @ensure_logged_in
    def get_sample_by_id(self, sampleId):
        with sc.ApiClient(self.clientconfig) as api_client:
            # Create an instance of the API class
            api = sc.SamplesApi(api_client)
            try:
                response = api.samples_controller_find_by_id(sampleId)
            except Exception as e:
                # TODO
                self.log.debug("Exception when calling Samples->samples_controller_find_by_id: %s\n" % e)
                return None
        return response

    def close(self):
        return
        self.session.close()
