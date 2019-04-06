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
#include "exceptions/page_not_pinned_exception.h"
#include "exceptions/page_pinned_exception.h"
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
		IndexMetaInfo *metaPage;
		Page *meta;
		this->bufMgr->readPage(this->file, this->file->getFirstPageNo(), meta);
		metaPage = (IndexMetaInfo *)meta;

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
		IndexMetaInfo *metaPage;
		Page *meta;
		this->bufMgr->allocPage(this->file, this->headerPageNum, meta);
		metaPage = (IndexMetaInfo *)meta;

		// Create a root node. This node is intialized as a leaf node.
		LeafNodeInt *rootNode;
		Page *root;
		this->bufMgr->allocPage(this->file, this->rootPageNum, root);
		rootNode = (LeafNodeInt *)root;
		rootNode->size = 0;
		rootNode->rightSibPageNo = 0;

		// Assign values to variables in metaPage
		strcpy(metaPage->relationName, relationName.c_str());
		metaPage->rootPageNo = this->rootPageNum;

		//Scan all tuples in the relation. Insert all tuples into the index.
		FileScan scn(relationName, bufMgrIn);
		try
		{
			RecordId scanRid;
			while (1)
			{
				scn.scanNext(scanRid);
				std::string recordStr = scn.getRecord();
				const char *record = recordStr.c_str();
				void *key = (void *)(record + this->attrByteOffset);
				insertEntry(key, scanRid);
			}
		}
		catch (const badgerdb::EndOfFileException &e)
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
	try
	{
		if (this->currentPageNum)
		{
			this->bufMgr->unPinPage(this->file, this->currentPageNum, false);
		}

		if (this->scanExecuting) {
			this->endScan();
		}

		this->bufMgr->flushFile(this->file);
		delete this->file;
		this->file = NULL;
	}
	catch(badgerdb::BadgerDbException e)
	{
		std::cerr << e.what() << '\n';
		throw e;
	}
	catch (PagePinnedException e){
		std::cerr << e.what() << '\n';
		throw e;
	}
	catch (PageNotPinnedException e){
		std::cerr << e.what() << '\n';
		throw e;
	}
	
}

// -----------------------------------------------------------------------------
// BTreeIndex::insertEntry
// -----------------------------------------------------------------------------
const void BTreeIndex::insertEntry(const void *key, const RecordId rid)
{
	Page *root;
	// Read root page to root
	this->bufMgr->readPage(this->file, this->rootPageNum, root);

	if (this->rootPageNum == 2)
	{ // This means root is a leaf.
		//LeafNodeInt* rootNode = (LeafNodeInt*) root;
		insertLeaf(key, rid, root->page_number());
		this->bufMgr->unPinPage(this->file, root->page_number(), false);
		return;
	}

	// The root is a nonleafNode
	//NonLeafNodeInt* rootNode = (NonLeafNodeInt*) root;
	PageId pageToInsert = FindPlaceHelper(key, root->page_number());
	insertLeaf(key, rid, pageToInsert);
	this->bufMgr->unPinPage(this->file, root->page_number(), false);
}

// -----------------------------------------------------------------------------
// BTreeIndex::FindPlaceHelper
// -----------------------------------------------------------------------------
PageId BTreeIndex::FindPlaceHelper(const void *key, PageId pageNo)
{
	Page *tmp;
	this->bufMgr->readPage(this->file, pageNo, tmp);
	NonLeafNodeInt *curNode = (NonLeafNodeInt *)tmp;

	// A non-leaf node must have size > 0
	for (int i = 0; i < curNode->size; i++)
	{
		// Insert in the front of larger key
		if (curNode->keyArray[i] >= *((int *)key))
		{
			// level = 1, the node is at the second-last level
			if (curNode->level)
			{
				this->bufMgr->unPinPage(this->file, pageNo, false);
				return (curNode->pageNoArray[i]);
			}
			// level = 0, the node is not at the second-last level of the tree
			else
			{
				this->bufMgr->unPinPage(this->file, pageNo, false);
				return (FindPlaceHelper(key, curNode->pageNoArray[i]));
			}
		}
	}

	// Insert at the end of array
	if (curNode->level)
	{
		this->bufMgr->unPinPage(this->file, pageNo, false);
		return (curNode->pageNoArray[curNode->size - 1]);
	}
	else
	{
		this->bufMgr->unPinPage(this->file, pageNo, false);
		return (FindPlaceHelper(key, curNode->pageNoArray[curNode->size - 1]));
	}
}

// -----------------------------------------------------------------------------
// BTreeIndex::insertLeaf
// -----------------------------------------------------------------------------
const void BTreeIndex::insertLeaf(const void *key, RecordId rid, PageId pageNo)
{
	Page *tmp;
	this->bufMgr->readPage(this->file, pageNo, tmp);
	LeafNodeInt *leafNode = (LeafNodeInt *)tmp;
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
		if (leafNode->size == this->leafOccupancy)
		{
			splitAndInsert(pageNo, key, rid);
			this->bufMgr->unPinPage(this->file, pageNo, true);
			return;
		}
		// No need to split
		for (int i = 0; i < leafNode->size; i++)
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
		if (!inserted)
		{
			leafNode->keyArray[leafNode->size - 1] = pair.key;
			leafNode->ridArray[leafNode->size - 1] = pair.rid;
			leafNode->size++;

			//------update parent's entry------//
			PageId parentPage = getParent(pageNo, key);
			Page *tmp;
			this->bufMgr->readPage(this->file, parentPage, tmp);
			NonLeafNodeInt *parent = (NonLeafNodeInt *)tmp;
			//update the corresponding entry in the parent node.
			int index = getIndexNonLeaf(parentPage, key);
			parent->keyArray[index] = leafNode->keyArray[leafNode->size - 1];
			this->bufMgr->unPinPage(this->file, parentPage, true);
		}
	}
	//Since inserted, the page is dirty
	this->bufMgr->unPinPage(this->file, pageNo, true);
}

// -----------------------------------------------------------------------------
// BTreeIndex::insertInternal
// -----------------------------------------------------------------------------
const void BTreeIndex::insertInternal(const void *key, PageId parentPageNo, PageId childPageNo)
{
	Page *tmp;
	this->bufMgr->readPage(this->file, parentPageNo, tmp);
	NonLeafNodeInt *parentNode = (NonLeafNodeInt *)tmp;
	// Get the index to insert
	int index = getIndexNonLeaf(parentPageNo, key);

	/*--- Check if need to split---*/
	if (parentNode->size == INTARRAYNONLEAFSIZE)
	{
		splitAndInsertInternal(parentPageNo, key, childPageNo);
	}
	// No need to split
	else
	{
		for (int j = parentNode->size - 1; j >= index; j--)
		{
			parentNode->keyArray[j + 1] = parentNode->keyArray[j];
			parentNode->pageNoArray[j + 1] = parentNode->pageNoArray[j];
		}
		//insert at index
		parentNode->keyArray[index] = *(int *)key;
		parentNode->pageNoArray[index] = childPageNo;
		parentNode->size++;
		//update the entry of grandparent if insert at the last entry of the parent
		if (index == parentNode->size - 1)
		{
			Page *tmp;
			PageId grandparentPage = getParent(parentPageNo, key);
			int parentIndex = getIndexNonLeaf(grandparentPage, key);
			this->bufMgr->readPage(this->file, grandparentPage, tmp);
			NonLeafNodeInt *grandparentNode = (NonLeafNodeInt *)tmp;
			grandparentNode->keyArray[parentIndex] = parentNode->keyArray[parentNode->size - 1];
			this->bufMgr->unPinPage(this->file, grandparentPage, true);
		}
	}
	this->bufMgr->unPinPage(this->file, parentPageNo, true);
}

// -----------------------------------------------------------------------------
// BTreeIndex::insertAndSplitInternal
// -----------------------------------------------------------------------------
const void BTreeIndex::splitAndInsertInternal(PageId pageToSplit, const void *key, PageId pageInPair)
{
	Page *tmp;
	this->bufMgr->readPage(this->file, pageToSplit, tmp);
	NonLeafNodeInt *nodeToSplit = (NonLeafNodeInt *)tmp;
	//------Construct a new page------//
	Page *newPage;
	PageId newPageNo;
	this->bufMgr->allocPage(this->file, newPageNo, newPage);
	NonLeafNodeInt *newNode = (NonLeafNodeInt *)newPage;
	// Update level
	newNode->level = nodeToSplit->level;
	// Create a pair to insert
	PageKeyPair<int> pair;
	pair.set(pageInPair, *((int *)key));

	/* Algorithm: first split the node into two nodes, then insert the keyPagePair to either node (call insertInternal).
	 * When splitting, push up the max key of the left node. Here call insertInternal again. In this way, all chained-splits
	 * should be handled during the recursive call between insertInternal() and insertAndSplitInternal().
	 * Note that if we reach the root and need to split the root, call the helper method splitRoot() and manually set the 
	 * new root to be the parent node.
	*/
	// New key is inserted into the new page (right node after split)
	if (pair.key > nodeToSplit->keyArray[nodeToSplit->size / 2])
	{
		// Move the floor (size / 2) of the original page to new page
		memcpy(newNode->keyArray, nodeToSplit->keyArray + sizeof(int) * (nodeToSplit->size + 1) / 2, (nodeToSplit->size) / 2);
		memcpy(newNode->pageNoArray, nodeToSplit->pageNoArray + sizeof(int) * (nodeToSplit->size + 1) / 2, (nodeToSplit->size) / 2);
		newNode->size = (nodeToSplit->size) / 2;
		nodeToSplit->size = (nodeToSplit->size + 1) / 2;
		// Edge case: Check if the nodeToSplit is the root node. If so, split the root
		PageId parentPageId = getParent(pageToSplit, key);
		if (!parentPageId)
		{
			splitRoot();
			parentPageId = this->rootPageNum;
		}
		// First push up the max value of the original node
		insertInternal(&nodeToSplit->keyArray[nodeToSplit->size - 1], parentPageId, pageToSplit);
		// Then add the new key into the right (new) node
		insertInternal(key, newPageNo, pair.pageNo);
	}
	// new key is inserted into the original page (left node after split)
	else
	{
		// Move the ceiling (size / 2) of the original page to new page
		memcpy(newNode->keyArray, nodeToSplit->keyArray + sizeof(int) * (nodeToSplit->size) / 2, (nodeToSplit->size + 1) / 2);
		memcpy(newNode->pageNoArray, nodeToSplit->pageNoArray + sizeof(int) * (nodeToSplit->size) / 2, (nodeToSplit->size + 1) / 2);
		newNode->size = (nodeToSplit->size + 1) / 2;
		nodeToSplit->size = (nodeToSplit->size) / 2;
		// Push up the max value of the original node
		// Check if the nodeToSplit is the root node. If so, split the root
		PageId parentPageId = getParent(pageToSplit, key);
		if (!parentPageId)
		{
			splitRoot();
			parentPageId = this->rootPageNum;
		}
		// Push up the max key of left    Note: When executing the following insertLeaf(), the entries of parent node will be updated
		insertInternal(&nodeToSplit->keyArray[nodeToSplit->size - 1], parentPageId, pageToSplit);
		// Then add the new key into the left (original) leaf
		insertInternal(key, pageToSplit, pair.pageNo);
	}

	this->bufMgr->unPinPage(this->file, pageToSplit, true);
	this->bufMgr->unPinPage(this->file, newPageNo, true);
}

// -----------------------------------------------------------------------------
// BTreeIndex::splitAndInsert
// -----------------------------------------------------------------------------
const void BTreeIndex::splitAndInsert(PageId pageNo, const void *key, RecordId rid)
{
	Page *tmp;
	this->bufMgr->readPage(this->file, pageNo, tmp);
	LeafNodeInt *pageToSplit = (LeafNodeInt *)tmp;
	RIDKeyPair<int> pair;
	pair.key = *((int *)key);
	pair.rid = rid;
	//------Construct a new page------//
	Page *newPage;
	PageId newPageId;
	this->bufMgr->allocPage(this->file, newPageId, newPage);
	LeafNodeInt *newNode = (LeafNodeInt *)newPage;
	// Update the right sibling page number
	newNode->rightSibPageNo = pageToSplit->rightSibPageNo;
	pageToSplit->rightSibPageNo = newPageId;

	// New key is inserted into the new page (right node after split)
	if (pair.key > pageToSplit->keyArray[pageToSplit->size / 2])
	{
		// Move the floor (size / 2) of the original page to new page
		memcpy(newNode->keyArray, pageToSplit->keyArray + sizeof(int) * (pageToSplit->size + 1) / 2, (pageToSplit->size) / 2);
		memcpy(newNode->ridArray, pageToSplit->ridArray + sizeof(int) * (pageToSplit->size + 1) / 2, (pageToSplit->size) / 2);
		newNode->size = (pageToSplit->size) / 2;
		pageToSplit->size = (pageToSplit->size + 1) / 2;
		// Edge case: Root is the leaf, and there is no parent(there is only one leaf node in the btree, and the pageNo = 2)
		PageId parentPageId = getParent(pageNo, key);
		if (!parentPageId)
		{
			splitRoot();
			parentPageId = this->rootPageNum;
		}
		// First push up the max value of the original node
		insertInternal(&pageToSplit->keyArray[pageToSplit->size - 1], parentPageId, pageNo);
		// Then add the new key into the right (new) leaf
		insertLeaf(key, rid, newPageId);
	}
	// new key is inserted into the original page (left node after split)
	else
	{
		// Move the ceiling (size / 2) of the original page to new page
		memcpy(newNode->keyArray, pageToSplit->keyArray + sizeof(int) * (pageToSplit->size) / 2, (pageToSplit->size + 1) / 2);
		memcpy(newNode->ridArray, pageToSplit->ridArray + sizeof(int) * (pageToSplit->size) / 2, (pageToSplit->size + 1) / 2);
		newNode->size = (pageToSplit->size + 1) / 2;
		pageToSplit->size = (pageToSplit->size) / 2;
		// Push up the max value of the original node
		PageId parentPageId = getParent(pageNo, key);
		if (!parentPageId)
		{
			splitRoot();
			parentPageId = this->rootPageNum;
		}
		// Push up the max key of the left leaf    Note: When executing the following insertLeaf(), the entries of parent node will be updated
		insertInternal(&pageToSplit->keyArray[pageToSplit->size - 1], parentPageId, pageNo);
		// Then add the new key into the left (original) leaf
		insertLeaf(key, rid, pageNo);
	}
	this->bufMgr->unPinPage(this->file, pageNo, true);
	this->bufMgr->unPinPage(this->file, newPageId, true);
}

// -----------------------------------------------------------------------------
// BTreeIndex::splitRoot
// -----------------------------------------------------------------------------
const void BTreeIndex::splitRoot()
{
	// First check if the root is a leaf. This is relevant to level assignment.
	bool rootIsLeaf = 0;
	if (this->rootPageNum == 2)
	{
		rootIsLeaf = 1;
	}
	Page *newRoot;
	PageId newRootId;
	this->bufMgr->allocPage(this->file, newRootId, newRoot);
	NonLeafNodeInt *newRootNode = (NonLeafNodeInt *)newRoot;
	this->rootPageNum = newRootId;
	// If before split the root is a leaf, then now the root is 1 level above the leaf so assign 1. Otherwise assign 0.
	newRootNode->level = rootIsLeaf ? 1 : 0;
	newRootNode->size = 0;
}

// -----------------------------------------------------------------------------
// BTreeIndex::getParent
// -----------------------------------------------------------------------------
PageId BTreeIndex::getParent(PageId childPageNo, const void *key)
{
	// This means there's no parent for the given node
	if (childPageNo == rootPageNum)
		return 0;
	PageId parentNo = rootPageNum; // Parent starts from the root
	while (1)
	{
		PageId childTmpId = getIndexNonLeaf(parentNo, key);
		if (childTmpId == childPageNo)
		{
			return parentNo;
		}
		parentNo = childTmpId;
	}
}

// -----------------------------------------------------------------------------
// BTreeIndex::getIndexNonLeaf
// -----------------------------------------------------------------------------
int BTreeIndex::getIndexNonLeaf(PageId pageNo, const void *key)
{
	Page *tmp;
	this->bufMgr->readPage(this->file, pageNo, tmp);
	NonLeafNodeInt *curNode = (NonLeafNodeInt *)tmp;
	// Check if the node is empty
	if (!curNode->size)
	{
		return 0;
	}
	// Find out the index
	for (int i = 0; i < curNode->size; i++)
	{
		if (curNode->keyArray[i] >= *((int *)key))
		{
			this->bufMgr->unPinPage(this->file, pageNo, false);
			return (i);
		}
	}
	this->bufMgr->unPinPage(this->file, pageNo, false);
	//Return the last index
	return (curNode->size);
}

// -----------------------------------------------------------------------------
// BTreeIndex::getIndexLeaf
// -----------------------------------------------------------------------------
int BTreeIndex::getIndexLeaf(PageId pageNo, const void *key)
{
	Page *tmp;
	this->bufMgr->readPage(this->file, pageNo, tmp);
	LeafNodeInt *curNode = (LeafNodeInt *)tmp;
	// Check if the node is empty
	if (!curNode->size)
	{
		return 0;
	}
	// Find out the index
	for (int i = 0; i < curNode->size; i++)
	{
		if (curNode->keyArray[i] >= *((int *)key))
		{
			this->bufMgr->unPinPage(this->file, pageNo, false);
			return (i);
		}
	}
	this->bufMgr->unPinPage(this->file, pageNo, false);
	//Return the last index
	return (curNode->size);
}

// -----------------------------------------------------------------------------
// BTreeIndex::startScan
// -----------------------------------------------------------------------------

const void BTreeIndex::startScan(const void *lowValParm,
								 const Operator lowOpParm,
								 const void *highValParm,
								 const Operator highOpParm)
{

	// TODO: If another scan is already executing, that needs to be ended here.
	if (scanExecuting || currentPageData != NULL)
	{
		currentPageData = NULL;
	}
	if ((lowOpParm != GT && lowOpParm != GTE) || (highOpParm != LT && highOpParm != LTE))
	{
		throw BadOpcodesException();
	}
	if (lowValParm > highValParm)
	{
		throw BadScanrangeException();
	}
	// set vars for scan
	lowValInt = *(int *)lowOpParm;
	highValInt = *(int *)highValParm;
	lowOp = lowOpParm;
	highOp = highOpParm;
	nextEntry = 0;
	scanExecuting = true;

	// read and unpin the root
	Page *root;
	this->bufMgr->readPage(this->file, rootPageNum, root);
	this->bufMgr->unPinPage(this->file, rootPageNum, false);
	// if root is a leaf, directly check whether the root's keys are in the range
	if (this->rootPageNum == 2)
	{
		LeafNodeInt *rootNode = (LeafNodeInt *)root;

		if (((highOpParm == LT) && (rootNode->keyArray[0] >= *(int *)highValParm)) ||
			((highOpParm == LTE) && (rootNode->keyArray[0] > *(int *)highValParm)) ||
			((lowOpParm == GT) && (rootNode->keyArray[rootNode->size - 1] < *(int *)lowValParm)) ||
			((lowOpParm == GTE) && (rootNode->keyArray[rootNode->size - 1] <= *(int *)lowValParm)))
		{
			throw NoSuchKeyFoundException();
		}
		// this->bufMgr->readPage(this->file, rootPageNum, root);
	}
	else // traver from root if it is a noneleaf
	{
		bool reachLeaf = false;
		NonLeafNodeInt *rootNode = (NonLeafNodeInt *)root;
		Page *currPage = root;
		NonLeafNodeInt *currNode = rootNode;

		// find the leaf node of contains lower bound
		while (!reachLeaf)
		{
			for (int i = 0; i < currNode->keyArray[rootNode->size - 1]; i++)
			{
				// ">" lowVal
				if (lowOpParm == GT && currNode->keyArray[i] > *(int *)lowValParm)
				{
					if (currNode->level == 1) // next level is leaf node
					{
						// assign the correct leaf page to be the first leaf page contain the lower bound
						this->bufMgr->readPage(this->file, currNode->pageNoArray[i], this->currentPageData);
						this->bufMgr->unPinPage(this->file, currNode->pageNoArray[i], false);
						reachLeaf = true;
						break;
					}
					// traverse to child if i th key is greater than the low bound
					this->bufMgr->readPage(this->file, currNode->pageNoArray[i], currPage);
					this->bufMgr->unPinPage(this->file, currNode->pageNoArray[i], false);
					currNode = (NonLeafNodeInt *)currPage;
					break;
				} // ">=" lowVal
				else if (lowOpParm == GTE && currNode->keyArray[i] >= *(int *)lowValParm)
				{
					if (currNode->level == 1)
					{
						this->bufMgr->readPage(this->file, currNode->pageNoArray[i], this->currentPageData);
						this->bufMgr->unPinPage(this->file, currNode->pageNoArray[i], false);
						reachLeaf = true;
						break;
					}
					// traverse to child if i th key is greater or equal than the low bound
					this->bufMgr->readPage(this->file, currNode->pageNoArray[i], currPage);
					this->bufMgr->unPinPage(this->file, currNode->pageNoArray[i], false);
					currNode = (NonLeafNodeInt *)currPage;
					break;
				}
			}
		}
		if (this->currentPageData == NULL)
		{
			// scanExecuting = false;
			throw NoSuchKeyFoundException();
		}
		// FIXME: cannot convert
		// currentPageNum = this->currentPageData->page_number;
	}
}

const bool BTreeIndex::inRange(int value)
{
	if (lowOp == GT && highOp == LT && value > lowValInt && value < highValInt)
		return true;
	if (lowOp == GT && highOp == LTE && value > lowValInt && value <= highValInt)
		return true;
	if (lowOp == GTE && highOp == LT && value >= lowValInt && value < highValInt)
		return true;
	if (lowOp == GTE && highOp == LTE && value >= lowValInt && value <= highValInt)
		return true;
	return false;
}
// -----------------------------------------------------------------------------
// BTreeIndex::scanNext
// -----------------------------------------------------------------------------

const void BTreeIndex::scanNext(RecordId &outRid)
{

	if (scanExecuting == false || currentPageData == NULL)
	{
		throw ScanNotInitializedException();
	}
	LeafNodeInt *currNode = (LeafNodeInt *)currentPageData;
	if (!inRange(currNode->keyArray[0]))
	{
		// scanExecuting = false;
		currentPageData = NULL;
		throw IndexScanCompletedException();
	}
	// fetch the record id if the entry match the scan
	if (inRange(currNode->keyArray[nextEntry]))
	{
		outRid = currNode->ridArray[nextEntry];
	}
	// move to the right sibling if the current page is entirely scannned
	if (nextEntry == leafOccupancy - 1)
	{
		if (currNode->rightSibPageNo == 0) //read the last leaf node
		{
			throw IndexScanCompletedException();
		}
		this->bufMgr->allocPage(this->file, currNode->rightSibPageNo, currentPageData);
		this->bufMgr->unPinPage(this->file, currNode->rightSibPageNo, false);
	}
	else // move to next entry of the same page
	{
		nextEntry++;
	}
}

// -----------------------------------------------------------------------------
// BTreeIndex::endScan
// -----------------------------------------------------------------------------
//
const void BTreeIndex::endScan()
{
	// TODO: criteria for not initialized scan
	if (scanExecuting)
	{
		throw ScanNotInitializedException();
	}
	// reset vars for scan
	currentPageData = NULL;
	scanExecuting = false;
	nextEntry = -1;
}

} // namespace badgerdb
