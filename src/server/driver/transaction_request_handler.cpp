#include "server/driver/transaction_request_handler.hpp"

#include "duckdb/common/string_util.hpp"
#include "duckdb/main/connection.hpp"
#include "server/validation.hpp"
#include "transaction_constants.hpp"
#include "utils/remote_error.hpp"

namespace duckdb {

namespace {

void SetUnknownTransactionResponse(distributed::DistributedResponse &response, const string &message) {
	response.set_success(false);
	response.set_error_message(message);
	response.mutable_transaction()->set_status(distributed::TRANSACTION_STATUS_UNKNOWN);
}

} // namespace

TransactionRequestHandler::TransactionRequestHandler(DistributedFlightServerTestState &test_state_p)
    : test_state(test_state_p) {
}

arrow::Status TransactionRequestHandler::Handle(const distributed::DistributedRequest &req,
                                                ClientRegistration &registration,
                                                distributed::DistributedResponse &resp) {
	test_state.RecordTransactionRequest();
	// Reject unspecified or unsupported lifecycle actions before reading transaction state.
	auto validation = ValidateRequest(req.transaction());
	if (!validation.ok()) {
		resp.set_success(false);
		resp.set_error_message(validation.message());
		return arrow::Status::OK();
	}
	// Both identifiers are required to distinguish a new lifecycle operation from its retries.
	if (req.transaction_id() == INVALID_TRANSACTION_ID || req.request_sequence() == INVALID_REQUEST_SEQUENCE) {
		resp.set_success(false);
		resp.set_error_message("Transaction identifier and request sequence must be specified");
		return arrow::Status::OK();
	}
	// BEGIN, COMMIT, and ROLLBACK belong to an explicit transaction; autocommit has no lifecycle RPCs.
	if (req.transaction_mode() != distributed::TRANSACTION_MODE_EXPLICIT) {
		resp.set_success(false);
		resp.set_error_message("Transaction lifecycle requests require EXPLICIT mode");
		return arrow::Status::OK();
	}

	// Tests use a delivered UNKNOWN response to exercise client-side outcome reconciliation.
	if (test_state.ShouldReturnUnknownTransactionResponse()) {
		SetUnknownTransactionResponse(resp, "Injected unknown transaction outcome");
		return arrow::Status::OK();
	}

	try {
		ExecuteAction(req, registration, resp);
	} catch (const std::exception &ex) {
		resp.set_success(false);
		resp.set_error_message(ex.what());
		ToRemoteError(ErrorData(ex), *resp.mutable_error());
		if (req.transaction().action() == distributed::TRANSACTION_ACTION_COMMIT) {
			registration.active_transaction_id = INVALID_TRANSACTION_ID;
			registration.finished_transaction_id = req.transaction_id();
			registration.finished_transaction_status = distributed::TRANSACTION_STATUS_ROLLED_BACK;
			registration.ClearRequestReplay();
			resp.mutable_transaction()->set_status(distributed::TRANSACTION_STATUS_ROLLED_BACK);
		} else {
			resp.mutable_transaction()->set_status(distributed::TRANSACTION_STATUS_UNKNOWN);
		}
		return arrow::Status::OK();
	}
	if (resp.success() && req.transaction().action() == distributed::TRANSACTION_ACTION_COMMIT &&
	    test_state.ShouldFailCommitResponse()) {
		return arrow::Status::IOError("Injected lost COMMIT response");
	}
	return arrow::Status::OK();
}

void TransactionRequestHandler::ExecuteAction(const distributed::DistributedRequest &req,
                                              ClientRegistration &registration,
                                              distributed::DistributedResponse &resp) {
	auto action = req.transaction().action();
	if (action == distributed::TRANSACTION_ACTION_BEGIN) {
		if (registration.active_transaction_id != INVALID_TRANSACTION_ID) {
			if (registration.active_transaction_id != req.transaction_id()) {
				resp.set_success(false);
				resp.set_error_message("Another transaction is already active on this client connection");
				resp.mutable_transaction()->set_status(distributed::TRANSACTION_STATUS_ACTIVE);
				return;
			}
			resp.mutable_transaction()->set_status(distributed::TRANSACTION_STATUS_ACTIVE);
		} else if (req.transaction_id() <= registration.finished_transaction_id) {
			resp.set_success(false);
			resp.set_error_message("Transaction identifier has already been finalized");
			if (req.transaction_id() == registration.finished_transaction_id) {
				resp.mutable_transaction()->set_status(registration.finished_transaction_status);
			} else {
				resp.mutable_transaction()->set_status(distributed::TRANSACTION_STATUS_UNKNOWN);
			}
			return;
		} else if (req.transaction_id() != registration.finished_transaction_id + 1) {
			SetUnknownTransactionResponse(resp, "Transaction identifier is not the next expected value");
			return;
		} else {
			registration.connection->BeginTransaction();
			registration.active_transaction_id = req.transaction_id();
			registration.ClearRequestReplay();
			registration.last_request_sequence = req.request_sequence();
			resp.mutable_transaction()->set_status(distributed::TRANSACTION_STATUS_ACTIVE);
		}
	} else {
		auto commit = action == distributed::TRANSACTION_ACTION_COMMIT;
		auto completed_status =
		    commit ? distributed::TRANSACTION_STATUS_COMMITTED : distributed::TRANSACTION_STATUS_ROLLED_BACK;
		auto action_name = commit ? "COMMIT" : "ROLLBACK";

		if (registration.active_transaction_id == req.transaction_id()) {
			if (req.request_sequence() <= registration.last_request_sequence) {
				SetUnknownTransactionResponse(
				    resp,
				    StringUtil::Format("%s request sequence is not newer than the previous operation", action_name));
				return;
			}
			if (commit) {
				registration.connection->Commit();
			} else {
				registration.connection->Rollback();
			}
			registration.active_transaction_id = INVALID_TRANSACTION_ID;
			registration.finished_transaction_id = req.transaction_id();
			registration.finished_transaction_status = completed_status;
			registration.ClearRequestReplay();
		} else if (registration.finished_transaction_id != req.transaction_id()) {
			SetUnknownTransactionResponse(
			    resp, StringUtil::Format("Remote Duckherder %s outcome is unknown: transaction state is unavailable",
			                             action_name));
			return;
		}
		resp.mutable_transaction()->set_status(registration.finished_transaction_status);
		if (registration.finished_transaction_status != completed_status) {
			resp.set_success(false);
			resp.set_error_message(StringUtil::Format("Remote Duckherder transaction was already %s",
			                                          commit ? "rolled back" : "committed"));
			return;
		}
	}
	resp.set_success(true);
}

} // namespace duckdb
