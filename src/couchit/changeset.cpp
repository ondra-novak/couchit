/*
 * changeSet.cpp
 *
 *  Created on: 11. 3. 2016
 *      Author: ondra
 */

#include <ctime>
#include "changeset.h"

#include "document.h"



#include "localView.h"
#include "validator.h"
//#include "validator.h"
namespace couchit {

Changeset::Changeset(CouchDB &db):db(db) {


}

Changeset& Changeset::update(const Document &document) {
	Value doc = document;
	Value id = doc["_id"];
	if (!id.defined())
		throw DocumentHasNoID( document);

	Value ctodel = document.getConflictsToDelete();
	if (ctodel.defined()) {
		Array ccdocs;
		for (auto &&item: ctodel) {
			ccdocs.add(Object("_id",id)
					("_rev",item)
					("_deleted",true));
		}
		ctodel = ccdocs;
	}

	scheduledDocs.push_back(ScheduledDoc(id.getString(),doc,ctodel));

	return *this;
}

Changeset& Changeset::update(const Value &document) {
	Value doc = document;
	Value id = doc["_id"];
	if (!id.defined())
		throw DocumentHasNoID( document);

	scheduledDocs.push_back(ScheduledDoc(id.getString(),doc,json::undefined));

	return *this;
}


template<typename X>
bool Changeset::docOrder(const X &a, const X &b) {
	return a.id < b.id;
}

Changeset& Changeset::commit(CouchDB& db) {
	if (scheduledDocs.empty()) return *this;


	std::sort(scheduledDocs.begin(),scheduledDocs.end(),docOrder<ScheduledDoc>);

	///if validator is not present, do not run empty cycle.

	Value now;

	Array docsToCommit;

	const Validator *v  = db.getValidator();
	for(auto &&item: scheduledDocs) {
		Value doc = item.data;

		if (v) {
			Validator::Result r = v->validateDoc(doc);;
			if (!r) throw ValidationFailedException(r);
		}

		Object adj(doc);

		bool hasTm = doc[CouchDB::fldTimestamp].defined();
		bool hasPrevRev =  doc[CouchDB::fldPrevRevision].defined();
		if (hasTm && !now.defined()) {
			 std::chrono::system_clock::time_point now = std::chrono::system_clock::now();
			  std::time_t now_c = std::chrono::system_clock::to_time_t(now);
			adj.set(CouchDB::fldTimestamp,Value(now_c));
		}
		if (hasPrevRev) {
			adj.set(CouchDB::fldPrevRevision, doc["_rev"]);
		}

		docsToCommit.add(adj);

	}
	for(auto &&item: scheduledDocs) {
		Value conflicts = item.conflicts;
		for (auto &&c: conflicts) {
			docsToCommit.add(c);
		}
	}

	commitedDocs.clear();
	scheduledDocs.clear();
	Value out = db.bulkUpload(docsToCommit);

	std::vector<UpdateException::ErrorItem> errors;

	auto siter = docsToCommit.begin();

	for (auto &&item : out) {
		if (siter == docsToCommit.end()) break;
		Value rev = item["rev"];
		Value err = item["error"];
		String id (item["id"]);
		StrViewA orgId = (*siter)["_id"].getString();
		if (orgId != id) {
			UpdateException::ErrorItem e;

			e.document = Object("_id",id)
							   ("_rev",rev);
			e.errorType = "internal_error";
			e.reason = "Server's response is out of sync";
			errors.push_back(e);
		} else if (err.defined()) {
			UpdateException::ErrorItem e;

			e.errorDetails = item;
			e.document = *siter;
			e.errorType = String(err);
			e.reason = String(item["reason"]);
			errors.push_back(e);
		} else if (rev.defined()) {
			commitedDocs.push_back(CommitedDoc(orgId, String(rev), *siter));
		}
		++siter;
	}

	if (!errors.empty()) throw UpdateException(errors);

	return *this;


}

Changeset::Changeset(const Changeset& other):db(other.db) {
}


void Changeset::revert(const StrViewA &docId) {
	auto iter = scheduledDocs.rbegin();
	while (iter != scheduledDocs.rend()) {
		if (iter->id == docId) {
			++iter;
			scheduledDocs.erase(iter.base());
			break;
		} else {
			++iter;
		}
	}
}


Changeset& Changeset::commit() {
	return commit(db);
}

Changeset& Changeset::erase(const String &docId, const String &revId) {


	Document doc;
	doc.setID(docId);
	doc.setRev(revId);
	doc.setDeleted();

	update(doc);

	return *this;
}

String Changeset::getCommitRev(const StrViewA& docId) const {


	auto f = std::lower_bound(commitedDocs.begin(), commitedDocs.end(), CommitedDoc(docId,String(),Value()),docOrder<CommitedDoc>);
	if (f != commitedDocs.end() && f->id == docId) {
		return f->newRev;
	} else {
		return String();
	}
}

Changeset& Changeset::preview(LocalView& view) {
	for (auto &&item:scheduledDocs) {
		view.updateDoc(item.data);
	}
	return *this;


}

String Changeset::getCommitRev(const Document& doc) const {
	return getCommitRev(doc.getID());
}

std::size_t Changeset::mark() const {
	return scheduledDocs.size();
}

void Changeset::revertToMark(std::size_t mark) {
	if (mark < scheduledDocs.size()) {
		scheduledDocs.resize(mark);
	}
}

Changeset::CommitedDoc::operator Document() const {
	Document d (doc);
	d.setRev(newRev);

	return d;
}


Value Changeset::getUpdatedDoc(const StrViewA& docId) const {
	auto f = std::lower_bound(commitedDocs.begin(), commitedDocs.end(), CommitedDoc(docId,String(),Value()),docOrder<CommitedDoc>);
	if (f != commitedDocs.end() && f->id == docId) {
		return f->doc.replace("_rev",f->newRev);
	} else {
		return Value();
	}
}


} /* namespace assetex */
