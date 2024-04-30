#include "catalog.h"
#include "query.h"


/*
 * Deletes records from a specified relation.
 *
 * Returns:
 * 	OK on success
 * 	an error code otherwise
 */

const Status QU_Delete(const string & relation, 
		       const string & attrName, 
		       const Operator op,
		       const Datatype type, 
		       const char *attrValue)
{
// part 6
	const char *filter;
	int intVal;
	float floatVal;
	Status status;
	RID rid;
	AttrDesc attrDesc;
	HeapFileScan *hfs;

	hfs = new HeapFileScan(relation, status);
	if (status != OK) {
		return status;
	}
	if (attrName.empty()) {
		status = hfs->startScan(0, 0, type, NULL, op);
	} else {
		status = attrCat->getInfo(relation, attrName, attrDesc);
		if (status != OK) {
			return status;
		}
		
		switch (type) {	
			case STRING: 
				status = hfs->startScan(attrDesc.attrOffset, attrDesc.attrLen, type, attrValue, op);
				break;

			case FLOAT: 
				floatVal = atof(attrValue);
				status = hfs->startScan(attrDesc.attrOffset, attrDesc.attrLen, type, (char *)&floatVal, op);
				break;

			case INTEGER: 
				intVal = atoi(attrValue);
				status = hfs->startScan(attrDesc.attrOffset, attrDesc.attrLen, type, (char *)&intVal, op);
				break;
		}
	}

	if (status != OK) {
		return status;
	}
	while (hfs->scanNext(rid) == OK) {
		status = hfs->deleteRecord();
		if (status != OK) {
			return status;
		}
	}
	status = hfs->endScan();
	if (status != OK) {
		return status;
	}
	delete hfs;
	return OK;


}


