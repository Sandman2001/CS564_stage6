#include "catalog.h"
#include "error.h"
#include "query.h"
#include <stdio.h>


/*
 * Inserts a record into the specified relation.
 *
 * Returns:
 * 	OK on success
 * 	an error code otherwise
 * This will insert the given values as a tuple into the relation relname. 
 * Note that the values of the attributes may need to be reordered before the insertion is performed in 
 * order to conform to the offsets specified for each attribute in the AttrCat table.  
 * You can do this by using memcpy to move each attribute in turn to its proper offset in a temporary array before calling insertRecord.
 * 
 * Insert a tuple with the given attribute values (in attrList) in relation. 
 * The value of the attribute is supplied in the attrValue member of the attrInfo structure. 
 * Since the order of the attributes in attrList[] may not be the same as in the relation, you might have to rearrange them before insertion. 
 * If no value is specified for an attribute, you should reject the insertion as Minirel does not implement NULLs.
 *
 */

const Status QU_Insert(const string & relation, 
	const int attrCnt, 
	const attrInfo attrList[])
{
// part 6
    printf("Doing QU_Insert\n");
    printf("QU_Insert: relation='%s' attrCnt=%d\n", relation.c_str(), attrCnt);
	Status status;
  	RelDesc rd;
  	AttrDesc ad;
	int attrcnt_in_rel;
	//check that relation exists	
	status = relCat->getInfo(relation, rd);
	if (status != OK)
		return status;
	printf(" relCat.getInfo OK: relName='%s' expectedAttrCnt=%d\n", rd.relName, rd.attrCnt);
	//check that attrCnt matches
	if (rd.attrCnt !=  attrCnt)
	{
		printf(" BADINSERTATTCNT: rd.attrCnt=%d input.attrCnt=%d\n", rd.attrCnt, attrCnt);
		return BADINSERTATTCNT;
	}
	
	//get all attribute info and check types and lengths
	AttrDesc* allAttrs = nullptr;
	status = attrCat->getRelInfo(relation, attrcnt_in_rel, allAttrs); //The attrs array is allocated by this function, but it should be deallocated by the caller.
	if (status != OK)
		return status;
	printf(" attrCat.getRelInfo OK: attrcnt_in_rel=%d\n", attrcnt_in_rel);
	for (int ai = 0; ai < attrcnt_in_rel; ai++) {
		printf("  Attr[%d] name='%s' type=%d len=%d off=%d\n", ai, allAttrs[ai].attrName, allAttrs[ai].attrType, allAttrs[ai].attrLen, allAttrs[ai].attrOffset);
	}
	//check that attrCnt matches
	if (attrcnt_in_rel != attrCnt) {
		printf(" BADINSERTATTCNT: catalogAttrCnt=%d inputAttrCnt=%d\n", attrcnt_in_rel, attrCnt);
		free(allAttrs);
		return BADINSERTATTCNT;
	}
	//create record data area
	char* data = new char[PAGESIZE]; //assume tuple fits in one page
	memset(data, 0, PAGESIZE);
	//for each attribute in relation, find it in attrList and copy value to data area
	for (int i = 0; i < attrCnt; i++) {
		ad = allAttrs[i];
		printf(" Mapping attr '%s' (type=%d len=%d off=%d)\n", ad.attrName, ad.attrType, ad.attrLen, ad.attrOffset);
		//find attribute in attrList
		bool found = false;
		for (int j = 0; j < attrCnt; j++) {
			if (strcmp(ad.attrName, attrList[j].attrName) == 0)
			{
				printf("  Found input '%s' (type=%d len=%d)\n", attrList[j].attrName, attrList[j].attrType, attrList[j].attrLen);
				//check type and copy value respecting declared attrLen
				if (ad.attrType != attrList[j].attrType) {
					printf("  BADINSERTPARM: type mismatch: relType=%d inputType=%d\n", ad.attrType, attrList[j].attrType);
					free(allAttrs);
					delete[] data;
					return BADINSERTPARM;
				}
				if (ad.attrType == STRING) {
					int providedLen = attrList[j].attrLen;
					if (providedLen < 0 && attrList[j].attrValue != nullptr) {
						providedLen = (int)strlen((const char*)attrList[j].attrValue);
					}
					if (providedLen < 0) providedLen = 0; // safety
					int copyLen = providedLen;
					if (copyLen > ad.attrLen) copyLen = ad.attrLen; // truncate if needed
					memcpy(data + ad.attrOffset, attrList[j].attrValue, copyLen);
					// pad remaining bytes with zeros if provided value shorter than schema length
					if (copyLen < ad.attrLen) {
						memset(data + ad.attrOffset + copyLen, 0, ad.attrLen - copyLen);
					}
					printf("   Copied STRING to off=%d copyLen=%d pad=%d\n", ad.attrOffset, copyLen, (ad.attrLen - copyLen));
				} else if (ad.attrType == INTEGER) {
					// Parse ASCII value to binary int then copy
					int v = 0;
					if (attrList[j].attrValue != nullptr)
						v = atoi((const char*)attrList[j].attrValue);
					memcpy(data + ad.attrOffset, &v, sizeof(int));
					printf("   Parsed INTEGER=%d, copied to off=%d bytes=%d (inputLen=%d)\n", v, ad.attrOffset, (int)sizeof(int), attrList[j].attrLen);
				} else if (ad.attrType == FLOAT) {
					// Parse ASCII value to binary float then copy
					float v = 0.0f;
					if (attrList[j].attrValue != nullptr)
						v = (float)atof((const char*)attrList[j].attrValue);
					memcpy(data + ad.attrOffset, &v, sizeof(float));
					printf("   Parsed FLOAT=%f, copied to off=%d bytes=%d (inputLen=%d)\n", v, ad.attrOffset, (int)sizeof(float), attrList[j].attrLen);
				}
				found = true;
				break;
			}
		}
		if (!found) {
			//attribute not found in attrList
			printf("  BADINSERTPARM: missing attribute '%s' in input list\n", ad.attrName);
			free(allAttrs);
			delete[] data;
			return BADINSERTPARM;
		}
	}
	//create record
	Record rec;
	rec.data = data;
	rec.length = 0;
	//calculate total length of record
	for (int i = 0; i < attrCnt; i++) {
		rec.length += allAttrs[i].attrLen;
	}
	printf(" Prepared record length=%d bytes\n", rec.length);
	//insert record into heap file
	HeapFile hf(relation, status);
	if (status != OK) {
		printf(" HeapFile open failed, status=%d\n", (int)status);
		free(allAttrs);
		delete[] data;
		return status;
	}
	RID rid;
	InsertFileScan ifs(relation, status);
	if (status != OK) {
		printf(" InsertFileScan open failed, status=%d\n", (int)status);
		free(allAttrs);
		delete[] data;
		return status;
	}
	status = ifs.insertRecord(rec, rid);
	printf(" insertRecord status=%d\n", (int)status);
	//clean up
	free(allAttrs);
	delete[] data;
	if (status != OK)
		return status;

	
return OK;

}

