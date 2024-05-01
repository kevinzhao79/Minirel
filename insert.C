#include "catalog.h"
#include "query.h"

//TODO: Kevin Zhao


/*
 * Inserts a record into the specified relation.
 *
 * Returns:
 * 	OK on success
 * 	an error code otherwise
 */

const Status QU_Insert(const string & relation, const int attrCnt, const attrInfo attrList[]) {

	Status status;
	int relAttrCnt;
	AttrDesc* relAttrDesc;

	// record that will be inserted at the end
	Record rec; 
    RID rid;
	// DEAR GODDESS ABOVE, DON'T FORGET TO INITIALIZE THIS
	int recLen = 0;

	status = attrCat->getRelInfo(relation, relAttrCnt, relAttrDesc);

	if (status != OK) {
		cerr << "Error in grabbing relation info." << endl;
		return RELNOTFOUND;
	}

	// check if the number of attributes requested to add matches the number of attributes in the relation
	if (attrCnt != relAttrCnt) {
		return BADCATPARM;
	}

	for (int i = 0; i < relAttrCnt; i++) {
        recLen += relAttrDesc[i].attrLen;
    }

	// TODO: What????
	InsertFileScan* ifs = new InsertFileScan(relation, status);

	if (status != OK) {
		cerr << "Error in instantiating InsertFileStatus" << endl;
		return UNIXERR;
	}

	rec.length = recLen;
    rec.data = (char*) malloc(recLen);

	// check if all types of the insert operation match the types of the relation
	for (int i = 0; i < relAttrCnt; i++) {

		// Info for the attributes of the relation
		AttrDesc relAttr = relAttrDesc[i];
		//printf("Got relation attribute %d\n",i);


		// for each attribute, check all insert attributes to find a matching one
		// because insert attributes in attrList[] are not aligned to relAttrDesc.
		bool found = false;
		for (int j = 0; j < attrCnt; j++) {

			// checks to make sure the name and type of attributes are equal
			bool nameCheck = false;
			bool typeCheck = false;

			// Info for the attributes of the to-be inserted record
			attrInfo insAttr = attrList[j];
			//printf("Got attribute info %d\n",j);

			// Use strcmp.
			//puts("strcmp");
			if (strcmp(relAttr.attrName,insAttr.attrName)==0) {nameCheck = true;}
			//puts("pass strcmp");

			if (relAttr.attrType == insAttr.attrType) {typeCheck = true;}
			//puts("pass attrType check");

			// move attrList[] attribute to the same order in the relation
			// This is not Python.
			if (nameCheck && typeCheck) {

				if (insAttr.attrValue == NULL) {
					cerr << "Attribute has a null value" << endl;
					return UNIXERR;
				}

				// TODO: WTF
				switch (insAttr.attrType)
                {

				case STRING:
					{ 
						//puts("a");
						char* value = (char*) insAttr.attrValue;
                    	memcpy((char*) rec.data + relAttr.attrOffset, value, relAttr.attrLen);
					}
					break;

                case INTEGER:
                    {
						//puts("b");
						int value = atoi((char*) insAttr.attrValue);
						//printf("value = %d, len = %d\n",value,relAttr.attrLen);
                    	memcpy((char*) rec.data + relAttr.attrOffset, (char*) &value, relAttr.attrLen);
						//puts("good");
					}
					break;

                case FLOAT:
                    {
						//puts("c");
						float value = atof((char*) insAttr.attrValue);
                    	memcpy((char*) rec.data + relAttr.attrOffset, (char*) &value, relAttr.attrLen);
					}
					break;
                
                }
				found = true;
			}

		}

		// THIS IS NOT PYTHON
		if (!found) {
			cerr << "Types of inserted record and relation do not match." << endl;
			return ATTRTYPEMISMATCH;
		}

	}

	// all types and names align, now insert
	ifs->insertRecord(rec, rid);

	return OK;
}

