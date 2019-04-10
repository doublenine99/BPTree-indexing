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
// #define debugBadIndex
// #define DEBUGINSERTENTRY
#define DEBUGFINDPLACEHELPER
#define DEBUGLEAFSPLIT
#define DEBUGINSERTLEAF
#define GETPRARENT


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
	outIndexName = idxStr.str();

	//------Initialize some members------//
	this->attrByteOffset = attrByteOffset;
	this->attributeType = attrType;
	this->leafOccupancy = INTARRAYLEAFSIZE;
	this->nodeOccupancy = INTARRAYNONLEAFSIZE;
	this->scanExecuting = false;
	this->nextEntry = -1;
	this->currentPageNum = 0;
	this->headerPageNum = 1;
	this->rootPageNum = 2;

	//-----Open the index file if exist; otherwise create a new index file with the name created.-----//
	if (File::exists(outIndexName))
	{
		BlobFile indexFile = BlobFile::open(outIndexName);
		this->file = &indexFile;
		//Read metaPage (first page) of the file
		Page *meta;
		this->bufMgr->readPage(this->file, this->file->getFirstPageNo(), meta);
		IndexMetaInfo* metaPage = (IndexMetaInfo*) meta;
		this->rootPageNum = metaPage->rootPageNo;
		//Unpin the meta page before throwing the exception
		try
		{
			this->bufMgr->unPinPage(this->file, this->file->getFirstPageNo(), false);
		}
		catch(const PageNotPinnedException& e)
		{
			std::cerr << "Constructor: closing meta page caused exception \t"<<  e.what() << '\n';
		}

#ifdef debugBadIndex
	std::cout<< "param: " << attrByteOffset <<" vs " << "opened: " << metaPage->attrByteOffset<< std::endl; 
	std::cout<< "param: " << relationName <<" vs " << "opened: " << metaPage->relationName<< std::endl; 
	std::cout<< "param: " << attrType <<" vs " << "opened: " << metaPage->attrType<< std::endl; 
#endif

		if (metaPage->attrByteOffset != attrByteOffset || metaPage->relationName != relationName || metaPage->attrType != attrType)
		{
			
			// try
			// {
			// 	this->bufMgr->unPinPage(this->file, this->file->getFirstPageNo(), false);
			// }
			// catch(const PageNotPinnedException& e)
			// {
			// 	std::cerr << "Constructor: closing meta page caused exception \t"<<  e.what() << '\n';
			// }
			throw new badgerdb::BadIndexInfoException("MetaInfo mismatch!");
		}
		// else
		// {
		// 	//Unpin the meta page
		// 	try
		// 	{
		// 		this->bufMgr->unPinPage(this->file, this->file->getFirstPageNo(), false);
		// 	}
		// 	catch(const PageNotPinnedException& e)
		// 	{
		// 		std::cerr << "Constructor: closing meta page caused exception \t"<<  e.what() << '\n';
		// 	}
		// }
	}
	//------Create new index file if the index file does not exist------//
	else
	{
		BlobFile indexFile = BlobFile::create(outIndexName);
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
#ifdef DEBUG
	std::cout<< "param: " << relationName <<" vs " << "opened: " << metaPage->relationName << std::endl; 
#endif
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
			try
			{
				this->bufMgr->unPinPage(this->file, this->headerPageNum, true);
				this->bufMgr->unPinPage(this->file, this->rootPageNum, true);
			}
			catch(const badgerdb::PageNotPinnedException& e) {
				std::cerr << "Constructor: closing meta/root page caused exception when creating new file \t"<<  e.what() << '\n';
			}
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
}

// -----------------------------------------------------------------------------
// BTreeIndex::insertEntry
// -----------------------------------------------------------------------------
const void BTreeIndex::insertEntry(const void *key, const RecordId rid)
{
	if (this->rootPageNum == 2)
	{ // This means root is a leaf.
		insertLeaf(key, rid, this->rootPageNum);
		return;
	}
#ifdef DEBUGINSERTLEAF
if (*(int*) key == 759 || *(int*) key == 760) {
	Page* tmp;
	this->bufMgr->readPage(this->file, this->rootPageNum, tmp);
	NonLeafNodeInt* root = (NonLeafNodeInt*) tmp;
	this->bufMgr->unPinPage(file, rootPageNum, false);
	std::cout<< "InsertEntry: size root: " << root->size << std::endl;
}
#endif
	// The root is a nonleafNode
	PageId pageToInsert = FindPlaceHelper(key, this->rootPageNum);
	insertLeaf(key, rid, pageToInsert);
}

// -----------------------------------------------------------------------------
// BTreeIndex::FindPlaceHelper
// -----------------------------------------------------------------------------
PageId BTreeIndex::FindPlaceHelper(const void *key, PageId pageNo)
{
	Page *tmp;
	this->bufMgr->readPage(this->file, pageNo, tmp);
	NonLeafNodeInt *curNode = (NonLeafNodeInt *)tmp;
	this->bufMgr->unPinPage(this->file, pageNo, false);
	// Find the index to insert in the page, and find the corresponding child page
	int index = getIndexNonLeaf(pageNo, key);
	PageId nextLevelPage = curNode->pageNoArray[index];
#ifdef DEBUGFINDPLACEHELPER
if (*(int*) key == 759 || *(int*) key == 760) {
	std::cout << "=====================================================================================================================================" << std::endl; 
	std::cout << "root page number: " << this->rootPageNum << std::endl;
	std::cout << "index in root node: " << index << std::endl; 
	std::cout << "page to insert into: " << nextLevelPage << std::endl;
	this->bufMgr->readPage(this->file, this->rootPageNum, tmp);
	NonLeafNodeInt* root = (NonLeafNodeInt*) tmp;
	std::cout << "root node size: " << root->size << std::endl;
	std::cout << "root node first key: " << root->keyArray[0] << std::endl;
	std::cout << "root node first page: " << root->pageNoArray[0] << std::endl; 
	std::cout << "root node second page: " << root->pageNoArray[1] << std::endl;
	std::cout << "root node last page: " << root->pageNoArray[root->size] << std::endl; 
	std::cout << "this key:  " << *(int*) key << std::endl; 
	this->bufMgr->unPinPage(this->file, this->rootPageNum, false);
	std::cout << "=====================================================================================================================================" << std::endl; 
}
#endif
	// If the page is one level above the leaf
	if (curNode->level == 1)
		return nextLevelPage;
	// Else, recursively find the right page to insert
	return FindPlaceHelper(key, nextLevelPage);
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
		return;
	}
	
#ifdef DEBUGINSERTLEAF
if (*(int*) key == 759 || *(int*) key == 760) {
	std::cout << "-----------InsertLeaf method starts here-----------\n" << "key: " << *(int*) key << " page num: "<< pageNo << std::endl;
	std::cout << "size: " << leafNode->size << std::endl;
}
#endif

	int index = getIndexLeaf(pageNo, key);
	
	/*---Check if need to split. If so, split and insert---*/
	if (leafNode->size == this->leafOccupancy)
	{
#ifdef DEBUGLEAFSPLIT
if (*(int*) key == 759 || *(int*) key == 760) {
	std::cout << "!!!!!!!!!!!!!!!!!!!!!!!!!!insertleaf Called splitAndInsert!!!!!!!!!!!!!!!!!!!!!!!!!!" << std::endl;
}
#endif
		splitAndInsert(pageNo, key, rid);
		this->bufMgr->unPinPage(this->file, pageNo, false);
		return;
	}
#ifdef DEBUGINSERTLEAF
if (*(int*) key == 759 || *(int*) key == 760) {
	std::cout << "No split\n" << std::endl;
	std::cout<< "index in leaf: " << index << std::endl;
	std::cout<< "BEFORE INSERT: size leaf: " << leafNode->size << std::endl;
	this->bufMgr->readPage(this->file, this->rootPageNum, tmp);
	NonLeafNodeInt* root = (NonLeafNodeInt*) tmp;
	this->bufMgr->unPinPage(file, rootPageNum, false);
	std::cout<< "BEFORE INSERT: size root: " << root->size << std::endl;
}
#endif

	// Move every entry from i to i+1 since we are inserting at i. In this case, no need to change parent's entry
	for (int j = leafNode->size - 1; j >= index; j--)
	{
		leafNode->keyArray[j + 1] = leafNode->keyArray[j];
		leafNode->ridArray[j + 1] = leafNode->ridArray[j];
	}
	//insert at index
	leafNode->keyArray[index] = pair.key;
	leafNode->ridArray[index] = pair.rid;
	leafNode->size++;

#ifdef DEBUGINSERTLEAF
if (*(int*) key == 759 || *(int*) key == 760) {
	std::cout<< "index: " << index << std::endl;
	std::cout<< "AFTER INSERT: size leaf: " << leafNode->size << std::endl;
	this->bufMgr->readPage(this->file, this->rootPageNum, tmp);
	NonLeafNodeInt* root = (NonLeafNodeInt*) tmp;
	this->bufMgr->unPinPage(file, rootPageNum, false);
	std::cout<< "AFTER INSERT: size root: " << root->size << std::endl;
}
#endif

	//------update parent's entry------//
	// PageId parentPage = getParent(pageNo, key);
// #ifdef DEBUGINSERTLEAF
// 		std::cout << "Parent page num:" << parentPage << std::endl;
// #endif
	// if (parentPage) updateParents(key, parentPage);
	
	//Since inserted, the page is dirty
	try
	{
		this->bufMgr->unPinPage(this->file, pageNo, true);
	}
	catch(const badgerdb::PageNotPinnedException& e)
	{
		std::cerr << "insertLeaf: exception thrown after all insertion \t" << e.what() << '\n';
	}
#ifdef DEBUGINSERTLEAF
	std::cout << "size: " << leafNode->size << std::endl;
	std::cout << "------------InsertLeaf method ends here------------\n" << std::endl;
#endif
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
			parentNode->pageNoArray[j + 2] = parentNode->pageNoArray[j + 1];
		}
		//insert at index
		parentNode->keyArray[index] = *(int *)key;
		parentNode->pageNoArray[index + 1] = childPageNo;
		parentNode->size++;
	}
	try
	{
		this->bufMgr->unPinPage(this->file, parentPageNo, true);
	}
	catch(const badgerdb::PageNotPinnedException& e)
	{
		std::cerr << "insertInternal: exception thrown when closing the parent page \t" << e.what() << '\n';
	}
	
}

// -----------------------------------------------------------------------------
// BTreeIndex::insertAndSplitInternal
// -----------------------------------------------------------------------------
const void BTreeIndex::splitAndInsertInternal(PageId leftPageNo, const void *key, PageId pageInPair)
{
	Page *tmp;
	this->bufMgr->readPage(this->file, leftPageNo, tmp);
	NonLeafNodeInt *leftNode = (NonLeafNodeInt *)tmp;
	//------Construct a new page------//
	Page *newPage;
	PageId newPageNo;
	this->bufMgr->allocPage(this->file, newPageNo, newPage);
	NonLeafNodeInt *newNode = (NonLeafNodeInt *)newPage;
	// Update level
	newNode->level = leftNode->level;
	// Create a pair to insert
	PageKeyPair<int> pair;
	pair.set(pageInPair, *((int *)key));

	int mid = (nodeOccupancy - 1) / 2;

	/* Algorithm: first split the node into two nodes, then insert the keyPagePair to either node (call insertInternal).
	 * When splitting, push up the max key of the left node. Here call insertInternal again. In this way, all chained-splits
	 * should be handled during the recursive call between insertInternal() and insertAndSplitInternal().
	 * Note that if we reach the root and need to split the root, call the helper method splitRoot() and manually set the 
	 * new root to be the parent node.
	*/
	
	memcpy(newNode->keyArray, leftNode->keyArray + sizeof(int) * (mid + 1), sizeof(int)*(nodeOccupancy - mid - 1));
	memcpy(newNode->pageNoArray, leftNode->pageNoArray + sizeof(int) * (mid + 1), sizeof(int)*(nodeOccupancy - mid));
	leftNode->size = mid;
	newNode->size = nodeOccupancy - mid - 1;
	
	PageId parentPageId = getParent(leftPageNo, key);
	// Edge case: Check if the leftNode is the root node. If so, split the root
	if (!parentPageId)
	{
		splitRoot();
		parentPageId = this->rootPageNum;
		Page* tmp;
		this->bufMgr->readPage(this->file, parentPageId, tmp);
		NonLeafNodeInt* root = (NonLeafNodeInt*) tmp;
		// Update root 
		root->size = 1;
		root->level = 0;
		root->keyArray[0] = leftNode->keyArray[mid];
		root->pageNoArray[0] = leftPageNo;
		root->pageNoArray[1] = newPageNo;
		this->bufMgr->unPinPage(this->file, parentPageId, true);
	}
	else
	{
		insertInternal(&leftNode->keyArray[mid], parentPageId, newPageNo);
	}
	
		//TODO: 想明白这东西怎么搞。subtree最右key拿上来push？？？？？？？？？？？？？？？？？？？？？？？？？？？？？？？？？？？？？？？？？？？？？？？？？？？？？？？？
	// Then add the new key into the right (new) node

	if (pair.key >= leftNode->keyArray[mid])
	{
		insertInternal(key, newPageNo, pair.pageNo);
	}
	else
	{
		insertInternal(key, leftPageNo, pair.pageNo);
	}
	try
	{
		this->bufMgr->unPinPage(this->file, leftPageNo, true);
		this->bufMgr->unPinPage(this->file, newPageNo, true);
	}
	catch(const badgerdb::PageNotPinnedException& e)
	{
		std::cerr << "splitAndInsertInternal: exception thrown when closing the original and new pages\t" << e.what() << '\n';
	}
	
}

// -----------------------------------------------------------------------------
// BTreeIndex::splitAndInsert
// -----------------------------------------------------------------------------
const void BTreeIndex::splitAndInsert(PageId pageNo, const void *key, RecordId rid)
{
#ifdef DEBUGLEAFSPLIT
	std::cout << "-----------splitAndInsert Method-----------\n" << "key: "<< *(int*) key  << " Page number:  " << pageNo << std::endl;
#endif
	Page *tmp;
	this->bufMgr->readPage(this->file, pageNo, tmp);
	LeafNodeInt *leftNode = (LeafNodeInt *)tmp;
	RIDKeyPair<int> pair;
	pair.key = *((int *)key);
	pair.rid = rid;

	//------Construct a new page------//
	Page *newPage;
	PageId newPageId;
	this->bufMgr->allocPage(this->file, newPageId, newPage);
	LeafNodeInt *newNode = (LeafNodeInt *)newPage;

	// Update the right sibling page number
	newNode->rightSibPageNo = leftNode->rightSibPageNo;
	leftNode->rightSibPageNo = newPageId;

	int size = leftNode->size; //Same as leaf occupancy
	int mid = size / 2 + size % 2;

	// Move the floor (size / 2) of the original page to new page
	memcpy(newNode->keyArray, leftNode->keyArray + sizeof(int) * mid, sizeof(int) * (size - mid));
	memcpy(newNode->ridArray, leftNode->ridArray + sizeof(int) * mid, sizeof(int) * (size - mid));
	leftNode->size = mid;
	newNode->size = size - mid;
	
	PageId parentPageId = getParent(pageNo, key);
	// Edge case: Root is the leaf, and there is no parent(there is only one leaf node in the btree, and the pageNo = 2)
	if (!parentPageId)
	{
		splitRoot();
		parentPageId = this->rootPageNum;
		Page* tmp;
		this->bufMgr->readPage(this->file, parentPageId, tmp);
		NonLeafNodeInt* root = (NonLeafNodeInt*) tmp;
		// Update root 
		root->size = 1;
		root->level = 1;
		root->keyArray[0] = leftNode->keyArray[mid];
		root->pageNoArray[0] = pageNo;
		root->pageNoArray[1] = newPageId;
		this->bufMgr->unPinPage(this->file, parentPageId, true);
	}
	else
	{
		insertInternal(key, parentPageId, newPageId);
	}

	// // Update the parents of left leaf with max key(since the new key will be inserted into right leaf, max key won't change)
	// updateParents((void*) &leftPageNo->keyArray[leftPageNo->size], parentPageId);

	// determine on which page to insert the new key
	if (*(int*) key >= leftNode->keyArray[mid]){
		// Then add the new key into the right (new) leaf
		insertLeaf(key, rid, newPageId);
	}
	else
	{
		insertLeaf(key, rid, pageNo);
	}

	//Unpin pages
	try
	{
		this->bufMgr->unPinPage(this->file, pageNo, true);
		this->bufMgr->unPinPage(this->file, newPageId, true);
	}
	catch(const badgerdb::PageNotPinnedException& e)
	{
		std::cerr << "splitAndInsert: exception thrown when closing the original and new leaf node\t" << e.what() << '\n';
	}
#ifdef DEBUGLEAFSPLIT
	std::cout << "original page number: " << pageNo << " | new page number: " << newPageId << std::endl;
	std::cout << "-----------splitAndInsert Method Ends here-----------\n" << std::endl;
#endif
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
	this->bufMgr->unPinPage(this->file, newRootId, true);

	Page* tmp;
	bufMgr->readPage(file, headerPageNum, tmp);
    IndexMetaInfo* metaPage = (IndexMetaInfo*) tmp;
    metaPage->rootPageNo = newRootId;
    bufMgr->unPinPage(file, headerPageNum, true);
}

// -----------------------------------------------------------------------------
// BTreeIndex::getParent
// -----------------------------------------------------------------------------
PageId BTreeIndex::getParent(PageId childPageNo, const void *key)
{
#ifdef GETPRARENT
	std::cout << "-----------getParent method starts here-----------" << std::endl;
	std::cout << "input page num: " << childPageNo << std::endl;
#endif
	// This means there's no parent for the given node
	if (childPageNo == rootPageNum){
#ifdef GETPRARENT
	std::cout << "-----------getParent method ends here----------- NO PARENT" << std::endl;
#endif
		return 0;
	}
	PageId parentNo = rootPageNum; // Parent starts from the root
#ifdef GETPRARENT	
	Page* tmp;
	std::cout << childPageNo << " ? " << rootPageNum << std::endl;
	this->bufMgr->readPage(this->file, rootPageNum, tmp);
	NonLeafNodeInt* root = (NonLeafNodeInt*) tmp;
	std::cout << "root size: " << root->size << std::endl;
	std::cout << "root 0: " << root->pageNoArray[0] << std::endl;
	std::cout << "root 1: " << root->pageNoArray[1] << std::endl;
	std::cout << "root level: " << root->level << std::endl;
#endif

	while (1)
	{
#ifdef GETPRARENT
	std::cout << "each iter parentNo: " << parentNo << std::endl;
#endif
		Page* tmp;
		this->bufMgr->readPage(this->file, parentNo, tmp);
		NonLeafNodeInt* parentCurNode = (NonLeafNodeInt*) tmp;
		PageId childCurNo = parentCurNode->pageNoArray[getIndexNonLeaf(parentNo, key)];
#ifdef GETPRARENT
	std::cout << "index in parent page: " << getIndexNonLeaf(parentNo, key) << std::endl;
	std::cout << "size of parent page: " << parentCurNode->size << std::endl;
	std::cout << "corresponding child page id: " <<childCurNo << std::endl;
#endif
		if (childCurNo == childPageNo)
		{
#ifdef GETPRARENT
	std::cout << "-----------getParent method ends here----------- Parent:" << parentNo << std::endl;
#endif
			return parentNo;
		}
		parentNo = childCurNo;
	}
}

// -----------------------------------------------------------------------------
// BTreeIndex::updateParents
// -----------------------------------------------------------------------------
void BTreeIndex::updateParents(const void* key, PageId pageNo)
{
	//------find parent node------//
	Page* tmp;
	PageId parentPage = getParent(pageNo, key);
	this->bufMgr->readPage(this->file, parentPage, tmp);
	NonLeafNodeInt* parentNode = (NonLeafNodeInt*) tmp;
	// Alg: find the parent node. If the key is in the last page in page array, meaning no need to update since the key is not in the 
	// left subtree. Otherwise compare the key with current key. If the current key is greater, do not update since there is a greater 
	// key in the left subtree of this node. Here we can stop since the further parents will have geater keys (larger left subtree with 
	// keys even greater). 
	// Otherwise update and keep updating the parents.

	//1. Check if we reach root
	if (parentPage == this->rootPageNum) {
		int index = getIndexNonLeaf(parentPage, key);
		// The key is on the last page in page array, so not in a left subtree of any entry. Do nothing.
		// The key is less than the current key on this position, do nothing.
		// Otherwise update.
		if(index != parentNode->size && *(int*) key > parentNode->keyArray[index])
		{
			parentNode->keyArray[index] = *(int*) key;
			this->bufMgr->unPinPage(this->file, parentPage, true);
		}
		this->bufMgr->unPinPage(this->file, parentPage, false);
	}
	// Not root case
	else
	{
		int index = getIndexNonLeaf(parentPage, key);
		// The key is less than the current key on this position, stop updating.
		// Otherwise update.
		if (*(int*) key <= parentNode->keyArray[index]){
			this->bufMgr->unPinPage(this->file, parentPage, false);
			return;
		}
		// The key is not on the last page in page array, not in a left subtree of any entry. keep updating parents.
		else if(index != parentNode->size)
		{
			parentNode->keyArray[index] = *(int*) key;
			this->bufMgr->unPinPage(this->file, parentPage, true);
		}
		this->bufMgr->unPinPage(this->file, parentPage, false);
		// Keep update parents
		updateParents(key, parentPage);
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
	try
	{
		this->bufMgr->unPinPage(this->file, pageNo, false);
	}
	catch(const badgerdb::PageNotPinnedException& e)
	{
		std::cerr << "getIndexNonLeaf: exception thrown after computing the index\t" << e.what() << '\n';
	}

#ifdef DEBUGINSERTLEAF
if (*(int*) key == 759 || *(int*) key == 760) {
	Page* tmp;
	this->bufMgr->readPage(this->file, this->rootPageNum, tmp);
	NonLeafNodeInt* root = (NonLeafNodeInt*) tmp;
	this->bufMgr->unPinPage(file, rootPageNum, false);
	std::cout<< "getIndexNonLeaf: size root: " << root->size << std::endl;
}
#endif

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
			return i;
		}
	}

	//Return the last index
	return curNode->size;
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
	try
	{
		this->bufMgr->unPinPage(this->file, pageNo, false);
	}
	catch(const badgerdb::PageNotPinnedException& e)
	{
		std::cerr << "getIndexLeaf: Exception thrown after computing the index\t" << e.what() << '\n';
	}

	if (!curNode->size)
	{
		return 0;
	}
	// Find out the index
	for (int i = 0; i < curNode->size; i++)
	{
		if (curNode->keyArray[i] >= *((int *)key))
		{
			// this->bufMgr->unPinPage(this->file, pageNo, false);
			return i;
		}
	}
	//Return the last index
	return curNode->size;
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

	if (*(int *)lowValParm >  *(int *)highValParm)
	{
		throw BadScanrangeException();
	}
	
	// set vars for scan
	lowValInt = *(int *)lowValParm;
	highValInt = *(int *)highValParm;
	lowOp = lowOpParm;
	highOp = highOpParm;
	nextEntry = 0;
	scanExecuting = true;
	std::cout <<"root num:"<< rootPageNum<< std::endl;
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
