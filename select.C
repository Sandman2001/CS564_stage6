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
 * 	A selection is implemented using a filtered HeapFileScan.  
 * 	The result of the selection is stored in the result relation called result (a  heapfile with this name will be created by the parser before QU_Select() is called).  
 * 	The project list is defined by the parameters projCnt and projNames.  
 *  Projection should be done on the fly as each result tuple is being appended to the result table.  
 * 	A final note: the search value is always supplied as the character string attrValue.  
 *  You should convert it to the proper type based on the type of attr. 
 *  You can use the atoi() function to convert a char* to an integer and atof() to convert it to a float.   
 *  If attr is NULL, an unconditional scan of the input table should be performed.
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
	//get attribute descriptors for projection attributes
	AttrDesc* projAttrs = new AttrDesc[projCnt];
	Status status;
	for (int i = 0; i < projCnt; i++) {
		status = attrCat->getInfo(projNames[i].relName, projNames[i].attrName, projAttrs[i]);
		if (status != OK) {
			delete[] projAttrs;
			return status;
		}
	}
	//get attribute descriptor for selection attribute, if any
	AttrDesc* selAttr = nullptr;
	if (attr != nullptr) {
		selAttr = new AttrDesc;
		status = attrCat->getInfo(attr->relName, attr->attrName, *selAttr);
		if (status != OK) {	
			delete[] projAttrs;
			delete selAttr;
			return status;
		}	
	}
    // compute output record length as sum of projected attribute lengths
    int outRecLen = 0;
    for (int i = 0; i < projCnt; i++) outRecLen += projAttrs[i].attrLen;

    //call ScanSelect to do the actual work
    status = ScanSelect(result, projCnt, projAttrs, selAttr, op, attrValue,
        outRecLen);
	//clean up
	delete[] projAttrs;
	if (selAttr != nullptr)
		delete selAttr;
	return status;	
}


const Status ScanSelect(const string & result, 
#include "stdio.h"
#include "stdlib.h"
			const int projCnt, 
			const AttrDesc projNames[],
			const AttrDesc *attrDesc, 
			const Operator op, 
			const char *filter,
			const int reclen)
{
    cout << "Doing HeapFileScan Selection using ScanSelect()" << endl;
    Status status;

    // Determine the relation to scan:
    // If a selection attribute is provided, use its relation name; otherwise,
    // use the relation from the first projection attribute.
    const char* scanRelName = (attrDesc != nullptr)
        ? attrDesc->relName
        : projNames[0].relName;

    HeapFileScan* hfs = new HeapFileScan(scanRelName, status);
    if (status != OK) {
        delete hfs;
        return status;
    }

    // Start scan: for unconditional scan (attrDesc == nullptr), use zero len/offset and a null filter.
    status = hfs->startScan(
        (attrDesc != nullptr) ? attrDesc->attrOffset : 0,
        (attrDesc != nullptr) ? attrDesc->attrLen    : 0,
        (attrDesc != nullptr) ? (Datatype)attrDesc->attrType : STRING,
        (attrDesc != nullptr) ? filter : NULL,
        op
    );
    if (status != OK) {
        delete hfs;
        return status;
    }

    // create inserter for result heap file
    InsertFileScan* resultInserter = new InsertFileScan(result, status);
    if (status != OK) {
        hfs->endScan();
        delete hfs;
        delete resultInserter;
        return status;
    }

    RID rid;
    Record rec;
    int recNum = 0;

    // scan through records
    while ((status = hfs->scanNext(rid)) == OK) {
        recNum++;

        // get record
        status = hfs->getRecord(rec);
        if (status != OK) {
            hfs->endScan();
            delete hfs;
            delete resultInserter;
            return status;
        }

        printf("[ScanSelect] Record %d: inRec.length=%d\n", recNum, (int)rec.length);

            // project attributes (pack sequentially from offset 0)
            char* data = new char[reclen];
            if (reclen > 0) memset(data, 0, reclen);
            int destOff = 0;

            for (int i = 0; i < projCnt; i++) {
                const int srcOff = projNames[i].attrOffset;
                const int len    = projNames[i].attrLen;
                const Datatype type = (Datatype)projNames[i].attrType;
                void* attrData = (char*)rec.data + srcOff;

            printf("  Attr[%d] %s.%s off=%d len=%d type=%d\n",
                   i, projNames[i].relName, projNames[i].attrName, srcOff, len, (int)type);

            // source bytes
            printf("    src bytes:");
            for (int b = 0; b < len; b++) printf(" %02X", ((unsigned char*)attrData)[b]);
            printf("\n");

            // typed view
            switch (type) {
                case INTEGER: {
                    int v = 0;
                    memcpy(&v, attrData, (int)sizeof(int) <= len ? sizeof(int) : len);
                    printf("    as int: %d\n", v);
                    break;
                }
                case FLOAT: {
                    float v = 0.0f;
                    memcpy(&v, attrData, (int)sizeof(float) <= len ? sizeof(float) : len);
                    printf("    as float: %f\n", v);
                    break;
                }
                case STRING: {
                    int slen = len;
                    char* sbuf = (char*)malloc(slen + 1);
                    memcpy(sbuf, attrData, slen);
                    sbuf[slen] = '\0';
                    printf("    as string: '%s'\n", sbuf);
                    free(sbuf);
                    break;
                }
                default:
                    printf("    (unknown type)\n");
            }

            // copy into destination buffer at packed offset
            if (reclen >= destOff + len) {
                memcpy(data + destOff, attrData, len);
            }
            printf("    copied to dest off=%d len=%d\n", destOff, len);
            destOff += len;
        }

        // final output buffer dump
        printf("  Output record len=%d hex:", reclen);
        for (int b = 0; b < reclen; b++) printf(" %02X", ((unsigned char*)data)[b]);
        printf("\n");

        // show projected fields from output buffer
        // show projected fields from output buffer using packed offsets
        destOff = 0;
        for (int i = 0; i < projCnt; i++) {
            const int off  = destOff;
            const int len  = projNames[i].attrLen;
            const Datatype type = (Datatype)projNames[i].attrType;
            void* p = data + off;

            printf("  Out Attr[%d] %s.%s:", i, projNames[i].relName, projNames[i].attrName);
            switch (type) {
                case INTEGER: {
                    int v = 0;
                    memcpy(&v, p, (int)sizeof(int) <= len ? sizeof(int) : len);
                    printf(" int=%d\n", v);
                    break;
                }
                case FLOAT: {
                    float v = 0.0f;
                    memcpy(&v, p, (int)sizeof(float) <= len ? sizeof(float) : len);
                    printf(" float=%f\n", v);
                    break;
                }
                case STRING: {
                    int slen = len;
                    char* sbuf = (char*)malloc(slen + 1);
                    memcpy(sbuf, p, slen);
                    sbuf[slen] = '\0';
                    printf(" str='%s'\n", sbuf);
                    free(sbuf);
                    break;
                }
                default:
                    printf(" (unknown type)\n");
            }
            destOff += len;
        }

        // create new record
        Record outRec;
        outRec.data = data;
        outRec.length = reclen;

        // insert record into result heap file
        RID outRid;
        status = resultInserter->insertRecord(outRec, outRid);
        printf("  Insert status=%d\n", (int)status);

        delete[] data;
        if (status != OK) {
            hfs->endScan();
            delete hfs;
            delete resultInserter;
            return status;
        }
    }
    hfs->endScan();
    delete hfs;
    delete resultInserter;
    return OK;
}
