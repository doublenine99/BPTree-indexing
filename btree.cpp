/**
 * @author See Contributors.txt for code contributors and overview of BadgerDB.
 *
 * @section LICENSE
 * Copyright (c) 2012 Database Group, Computer Sciences Department, University of Wisconsin-Madison.
 */

#include "btree.h"
#include "filescan.h"
#include "exceptions/bad_index_info_exception.h"
#include "exceptions/bad_opcodes_exception.h"
#include "exceptions/bad_scanrange_exception.h"
#include "exceptions/no_such_key_found_exception.h"
#include "exceptions/scan_not_initialized_exception.h"
#include "exceptions/index_scan_completed_exception.h"
#include "exceptions/file_not_found_exception.h"
#include "exceptions/end_of_file_exception.h"

//#define DEBUG

namespace badgerdb
{

/**
 * True if root is a leaf, otherwise false
 */
bool rootIsLeaf = true;

// -----------------------------------------------------------------------------
// BTreeIndex::BTreeIndex -- Constructor
// -----------------------------------------------------------------------------

BTreeIndex::BTreeIndex(const std::string &relationName,
					   std::string &outIndexName,
					   BufMgr *bufMgrIn,
					   const int attrByteOffset,
					   const Datatype attrType)
{
	this->bufMgr = bufMgrIn;
	//------Create the name of index file------//
	std::ostringstream idxStr;
	idxStr << relationName << '.' << attrByteOffset;
	std::string indexName = idxStr.str();
	outIndexName = indexName;

	//------Initialize some members------//
	this->attrByteOffset = attrByteOffset;
	this->attributeType = attrType;
	this->leafOccupancy = INTARRAYLEAFSIZE;
	this->nodeOccupancy = INTARRAYNONLEAFSIZE;
	this->scanExecuting = false;
    this->nextEntry = -1;
    this->currentPageNum = 0;

	//-----Open the index file if exist; otherwise create a new index file with the name created.-----//
	if (File::exists(indexName)) 
	{
		BlobFile indexFile = BlobFile::open(indexName);
		this->file = &indexFile;
		//Read metaPage (first page) of the file
		IndexMetaInfo* metaPage; 
		Page* meta;
		this->bufMgr->readPage(this->file, this->file->getFirstPageNo(), meta);
		metaPage = (IndexMetaInfo*) meta;
		
		if (metaPage->attrByteOffset != attrByteOffset || metaPage->relationName != relationName || metaPage->attrType != attrType)
		{
			//Unpin the meta page before throwing the exception
			this->bufMgr->unPinPage(this->file, this->file->getFirstPageNo(), meta);
			throw new badgerdb::BadIndexInfoException("MetaInfo mismatch!");
		}
		else
		{
			//Unpin the meta page
			this->bufMgr->unPinPage(this->file, this->file->getFirstPageNo(), meta);
		}
	}
	//------Create new index file if the index file does not exist------//
	else
	{
		BlobFile indexFile = BlobFile::create(indexName);
		this->file = &indexFile;
		//Create metainfo Page
		IndexMetaInfo* metaPage; 
		Page* meta;
		this->bufMgr->allocPage(this->file, this->headerPageNum, meta);
		metaPage = (IndexMetaInfo*) meta;

		// Create a root node. This node is intialized as a leaf node.
		LeafNodeInt* rootNode;
		Page* root;
		this->bufMgr->allocPage(this->file, this->rootPageNum, root);
		rootNode = (LeafNodeInt*) root;
		rootNode->size = 0;
		rootNode->rightSibPageNo = NULL; //TODO: might cause segfault
		
		// Assign values to variables in metaPage
		strcpy(metaPage->relationName, relationName.c_str());
		metaPage->rootPageNo = this->rootPageNum;

		//Scan all tuples in the relation. Insert all tuples into the index.
		FileScan scn(relationName, bufMgrIn);
		try
		{
			RecordId scanRid;
			while(1)
			{
				scn.scanNext(scanRid);
				std::string recordStr = scn.getRecord();
				const char *record = recordStr.c_str();
				void* key = (void*)(record + this->attrByteOffset);
				insertEntry(key, scanRid);
			}
		}
		catch(const badgerdb::EndOfFileException& e)
		{
			std::cout << "All records has been read" << '\n';
			//Unpin pages
			this->bufMgr->unPinPage(this->file, this->headerPageNum, meta);
			this->bufMgr->unPinPage(this->file, this->rootPageNum, root);
		}
		
	}

}

// -----------------------------------------------------------------------------
// BTreeIndex::~BTreeIndex -- destructor
// -----------------------------------------------------------------------------

BTreeIndex::~BTreeIndex()
{
}

// -----------------------------------------------------------------------------
// BTreeIndex::insertEntry
// -----------------------------------------------------------------------------

const void BTreeIndex::insertEntry(const void *key, const RecordId rid)
{
	Page* root;
	// Read root page to root
	this->bufMgr->readPage(this->file, this->rootPageNum, root);

	if (this->rootPageNum == 2) { // This means root is a leaf.
		LeafNodeInt* rootNode = (LeafNodeInt*) root;
		insertLeaf(key, rid, root->page_number);
	}
	

	// the root is a nonleafNode
	NonLeafNodeInt* rootNode = (NonLeafNodeInt*) root;
	PageId pageToInsert = FindPlaceHelper(key, root->page_number);

	//unpin page root
	this->bufMgr->unPinPage(this->file, root->page_number, false);
}

// -----------------------------------------------------------------------------
// BTreeIndex::FindPlaceHelper
// -----------------------------------------------------------------------------
PageId BTreeIndex::FindPlaceHelper(const void *key, PageId pageNo)
{
	Page* tmp;
	this->bufMgr->readPage(this->file, pageNo, tmp);
	NonLeafNodeInt* curNode = (NonLeafNodeInt*) tmp;

	// A non-leaf node must have size > 0
	for(int i = 0; i < curNode->size; i++)
	{
		// Insert in the front of larger key
		if (curNode->keyArray[i] >= *((int *) key))
		{
			// level = 1, the node is at the second-last level
			if (curNode->level){
				this->bufMgr->unPinPage(this->file, pageNo, false);
				return(curNode->pageNoArray[i]);
			}
			// level = 0, the node is not at the second-last level of the tree
			else
			{
				this->bufMgr->unPinPage(this->file, pageNo, false);	
				return(FindPlaceHelper(key, curNode->pageNoArray[i]));
			}
		} 
	}

	// Insert at the end of array
	if (curNode->level){
		this->bufMgr->unPinPage(this->file, pageNo, false);
		return(curNode->pageNoArray[curNode->size - 1]);
	}
	else
	{
		this->bufMgr->unPinPage(this->file, pageNo, false);
		return(FindPlaceHelper(key, curNode->pageNoArray[curNode->size - 1]));
	}
		
}

// -----------------------------------------------------------------------------
// BTreeIndex::insertLeafNode
// -----------------------------------------------------------------------------
const void BTreeIndex::insertLeaf(const void* key, RecordId rid, PageId pageNo)
{
	Page* tmp;
	this->bufMgr->readPage(this->file, pageNo, tmp);
	LeafNodeInt* leafNode = (LeafNodeInt*) tmp;
	RIDKeyPair<int> pair;
	pair.set(rid, *((int *)key));

	// If the leaf node is empty
	if (!leafNode->size)
	{
		leafNode->keyArray[leafNode->size] = pair.key;
		leafNode->ridArray[leafNode->size++] = pair.rid;
	}
	else
	{
		bool inserted = 0;
		/*---Check if need to split. If so, split and insert---*/
		if (leafNode->size == this->leafOccupancy) {	
			splitAndInsert(pageNo, key, rid);
			this->bufMgr->unPinPage(this->file, pageNo, true);
			return;
		}
		// No need to split
		for(int i = 0; i < leafNode->size; i++)
		{
			if (leafNode->keyArray[i] > pair.key)
			{
				// Move every entry from i to i+1 since we are inserting at i. In this case, no need to change parent's entry
				for (int j = leafNode->size - 1; j >= i; j--)
				{
					leafNode->keyArray[j + 1] = leafNode->keyArray[j];
					leafNode->ridArray[j + 1] = leafNode->ridArray[j];
				}			
				//insert at i
				leafNode->keyArray[i] = pair.key;
				leafNode->ridArray[i] = pair.rid;
				leafNode->size++;
				inserted = 1;
				break;
			}
		}
		// Insert at the last and change the parent's entry 
		if (!inserted){
			leafNode->keyArray[leafNode->size - 1] = pair.key;
			leafNode->ridArray[leafNode->size - 1] = pair.rid;
			leafNode->size++;

			//------update parent's entry------//
			PageId parentPage = getParent(pageNo, key);
			Page* tmp;
			this->bufMgr->readPage(this->file, parentPage, tmp);
			NonLeafNodeInt* parent = (NonLeafNodeInt*) tmp;
			//update the corresponding entry in the parent node.
			//需要更改pageid吗》？？？？？？？？？？？？？？？？？？？？？？？可能有bug
			int index = getIndexNonLeaf(parentPage, key);
			parent->keyArray[index] = leafNode->keyArray[leafNode->size - 1];
		}
	}	
	//Since inserted, the page is dirty
	this->bufMgr->unPinPage(this->file, pageNo, true);
}

// -----------------------------------------------------------------------------
// BTreeIndex::insertInternal
// -----------------------------------------------------------------------------
const void BTreeIndex::insertInternal(const void* key, PageId parentPageNo, PageId childPageNo){
	Page* tmp;
	this->bufMgr->readPage(this->file, parentPageNo, tmp);
	NonLeafNodeInt* internalNode = (NonLeafNodeInt*) tmp;
	// Get the index to insert
	int index = getIndexNonLeaf(parentPageNo, key); 

	/*--- Check if need to split---*/
	if (internalNode->size == INTARRAYNONLEAFSIZE)
	{
		
	}
	// No need to split
	else
	{
		for (int j = internalNode->size - 1; j >= index; j--)
		{
			internalNode->keyArray[j + 1] = internalNode->keyArray[j];
			internalNode->pageNoArray[j + 1] = internalNode->pageNoArray[j];
		}			
		//insert at index
		internalNode->keyArray[index] = *(int *)key;
		internalNode->pageNoArray[index] = childPageNo;
		internalNode->size++;
	}

	this->bufMgr->unPinPage(this->file, parentPageNo, true);
}

// -----------------------------------------------------------------------------
// BTreeIndex::splitInternal
// -----------------------------------------------------------------------------
const void BTreeIndex::splitInternal(PageId pageNo, const void* key){

}

// -----------------------------------------------------------------------------
// BTreeIndex::splitAndInsert
// -----------------------------------------------------------------------------
const void BTreeIndex::splitAndInsert(PageId pageNo, const void* key, RecordId rid)
{
	Page* tmp;
	this->bufMgr->readPage(this->file, pageNo, tmp);
	LeafNodeInt* pageToSplit = (LeafNodeInt*) tmp;
	RIDKeyPair<int>* pair;
	pair->key = *((int*) key);
	pair->rid = rid;
	//------Construct a new page------//
	Page* newPage;
	PageId newPageId;
	this->bufMgr->allocPage(this->file, newPageId,newPage);
	LeafNodeInt* newNode = (LeafNodeInt*) newPage;
	//update the right sibling page number
	newNode->rightSibPageNo = pageToSplit->rightSibPageNo;
	pageToSplit->rightSibPageNo = newNode->rightSibPageNo;

	//new key is inserted into the new page (right node after split)
	if (pair->key > pageToSplit->keyArray[pageToSplit->size / 2]) {
		// Move the floor (size / 2) of the original page to new page
		memcpy(newNode->keyArray, pageToSplit->keyArray + sizeof(int) * (pageToSplit->size + 1) / 2, (pageToSplit->size) / 2);
		memcpy(newNode->ridArray, pageToSplit->ridArray + sizeof(int) * (pageToSplit->size + 1) / 2, (pageToSplit->size) / 2);
		newNode->size = (pageToSplit->size) / 2;
		pageToSplit->size = (pageToSplit->size + 1) / 2;
		// Edge case: Root is leaf, and there is no parent(there is only one leaf node in the btree, and the pageNo = 2)
		splitRoot();
		// first push up the max value of the original node
		insertInternal(&pageToSplit->keyArray[pageToSplit->size - 1], getParent(pageNo, key), pageNo);
		// Then add the new key into the leaf
		insertLeaf(key, rid, newPageId);
	}
	// new key is inserted into the original page (left node after split)
	else{
		// Move the ceiling (size / 2) of the original page to new page
		memcpy(newNode->keyArray, pageToSplit->keyArray + sizeof(int) * (pageToSplit->size) / 2, (pageToSplit->size + 1) / 2);
		memcpy(newNode->ridArray, pageToSplit->ridArray + sizeof(int) * (pageToSplit->size) / 2, (pageToSplit->size + 1) / 2);
		newNode->size = (pageToSplit->size + 1) / 2;
		pageToSplit->size = (pageToSplit->size) / 2;
		// push up the max value of the original node
		//如果root是leaf??????????????????????????????????????????????????????????????????????????????
		splitRoot();
		//push up the max key of left!!!!!!!!!!!!!!!!!!!!!!!!!!! 加的可能是最大的 ！！！！有bug
		insertInternal(&pageToSplit->keyArray[pageToSplit->size - 1], getParent(pageNo, key), pageNo);
		
		insertLeaf(key, rid, pageNo);
	}
	this->bufMgr->unPinPage(this->file, pageNo, true);
	this->bufMgr->unPinPage(this->file, newPageId, true);
}

// -----------------------------------------------------------------------------
// BTreeIndex::getParent
// -----------------------------------------------------------------------------
PageId BTreeIndex::getParent(PageId childPageNo, const void* key){
	PageId parentNo = rootPageNum; 	// Parent starts from the root
	while(1)
	{
		PageId childTmpId = getIndexNonLeaf(parentNo, key);
		if(childTmpId == childPageNo)
		{
			return parentNo;
		} 
		parentNo = childTmpId;
	}
}

// -----------------------------------------------------------------------------
// BTreeIndex::getIndexNonLeaf
// -----------------------------------------------------------------------------
int BTreeIndex::getIndexNonLeaf(PageId pageNo, const void* key){
	Page* tmp;
	this->bufMgr->readPage(this->file, pageNo, tmp);
	NonLeafNodeInt* curNode = (NonLeafNodeInt*) tmp;
	// Check if the node is empty
	if(!curNode->size){
		return 0;
	}
	// find out the index
	for(int i = 0; i < curNode->size; i++)
	{
		if (curNode->keyArray[i] >= *((int*) key))
		{
			this->bufMgr->unPinPage(this->file, pageNo, false);
			return(i);
		} 
	}
	this->bufMgr->unPinPage(this->file, pageNo, false);
	//Return the last index
	return(curNode->size);
}

// -----------------------------------------------------------------------------
// BTreeIndex::getIndexLeaf
// -----------------------------------------------------------------------------
int BTreeIndex::getIndexLeaf(PageId pageNo, const void* key){
	Page* tmp;
	this->bufMgr->readPage(this->file, pageNo, tmp);
	LeafNodeInt* curNode = (LeafNodeInt*) tmp;

	for(int i = 0; i < curNode->size; i++)
	{
		if (curNode->keyArray[i] >= *((int*) key))
		{
			this->bufMgr->unPinPage(this->file, pageNo, false);
			return(i);
		} 
	}
	this->bufMgr->unPinPage(this->file, pageNo, false);
	//Return the last index
	return(curNode->size);
}


// -----------------------------------------------------------------------------
// BTreeIndex::startScan
// -----------------------------------------------------------------------------

const void BTreeIndex::startScan(const void *lowValParm,
								 const Operator lowOpParm,
								 const void *highValParm,
								 const Operator highOpParm)
{
}

// -----------------------------------------------------------------------------
// BTreeIndex::scanNext
// -----------------------------------------------------------------------------

const void BTreeIndex::scanNext(RecordId &outRid)
{
}

// -----------------------------------------------------------------------------
// BTreeIndex::endScan
// -----------------------------------------------------------------------------
//
const void BTreeIndex::endScan()
{
}

} // namespace badgerdb
