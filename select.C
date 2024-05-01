#include "catalog.h"
#include "query.h"


// forward declaration
const Status ScanSelect(const string & result, 
			const int projCnt, 
			const AttrDesc projNames[],
			const AttrDesc *attrDesc, 
			const Operator op, 
			const char *filter,
			const int reclen);

/*
 * Selects records from the specified relation.
 *
 * Returns:
 * 	OK on success
 * 	an error code otherwise
 */

const Status QU_Select(const string & result, 
		       const int projCnt, 
		       const attrInfo projNames[],
		       const attrInfo *attr, 
		       const Operator op, 
		       const char *attrValue)
{
   // Qu_Select sets up things and then calls ScanSelect to do the actual work
    cout << "Doing QU_Select " << endl;
	// Do something like ordering the projname first?
	
	Status status;
	AttrDesc attrDesc[projCnt];
	for(int i = 0; i < projCnt; ++i) {
		status = attrCat->getInfo(projNames[i].relName,
									projNames[i].attrName,
									attrDesc[i]);
		if(status != OK) {
			return status;
		}
	}

	// Get record length
	int reclen = 0;
	for(int i = 0; i < projCnt; ++i) {
		reclen += attrDesc[i].attrLen;
	}

	//printf("tuple select produced %d result tuples \n", resultTupCnt);
    
	if(attr == NULL) {
		return ScanSelect(result,
							projCnt,
							attrDesc,
							NULL,
							op,
							NULL,
							reclen);
	} else {
		AttrDesc attrDesc1;
		status = attrCat->getInfo(attr->relName, attr->attrName, attrDesc1);
		if (status != OK) return status;

		// convert the filter into an appropriate type
		int len = attrDesc1.attrLen;
		switch (attrDesc1.attrType)
		{
		case INTEGER:
			{
			int value = atoi(attrValue);
			return ScanSelect(result,
							projCnt,
							attrDesc,
							&attrDesc1,
							op,
							(char*) &value,
							reclen);
			}
			break;
		
		case STRING:
			return ScanSelect(result,
							projCnt,
							attrDesc,
							&attrDesc1,
							op,
							attrValue,
							reclen);
			break;

		case FLOAT:
			{
			float value = atof(attrValue);
			return ScanSelect(result,
							projCnt,
							attrDesc,
							&attrDesc1,
							op,
							(char*) &value,
							reclen);
			}
			break;

		default:
			break;
		}
	}
}


#include "stdio.h"
#include "stdlib.h"
const Status ScanSelect(const string & result, 
			const int projCnt, 
			const AttrDesc projNames[],
			const AttrDesc *attrDesc, 
			const Operator op, 
			const char *filter,
			const int reclen)
{
    cout << "Doing HeapFileScan Selection using ScanSelect()" << endl;

	Status status;
	// Open the result relation,
	// Prepare to insert records
	InsertFileScan resultRel(result, status);
	int resultTupCnt = 0;
	if (status != OK) return status;

	char outputData[reclen];
	Record outputRec;
	RID outRID;
	outputRec.data = (void*)outputData;
	outputRec.length = reclen;

	HeapFileScan scanner(string(projNames[0].relName), status);
	if(status != OK) return status;
	
	if(attrDesc == NULL) {
		// Full scan
		
		// Initialize scanner
		// If filter = NULL, then it marks all records as matched.
		status = scanner.startScan(0,0,STRING,NULL,EQ);
		if(status != OK) return status;

	} else {
		// Initialize Scanner
		status = scanner.startScan(attrDesc->attrOffset,
									attrDesc->attrLen,
									(Datatype) attrDesc->attrType,
									filter,
									op);
	}

	RID rec;
	while(scanner.scanNext(rec) == OK) {
		Record tempRec;
		status = scanner.getRecord(tempRec);
		assert(status == OK);
		
		// Look through each projection attribute
		// copy the relevant entry to the corresponding position
		int outputOffset = 0;
		for(int i = 0; i < projCnt; ++i) {
			memcpy((char*)outputRec.data+outputOffset,
					(char*)tempRec.data+projNames[i].attrOffset,
					projNames[i].attrLen);
			outputOffset += projNames[i].attrLen;
		}

		// Insert the record
		status = resultRel.insertRecord(outputRec, outRID);
		assert(status == OK);
		resultTupCnt++;
	}

	return OK;
}
