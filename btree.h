/**
 * @author See Contributors.txt for code contributors and overview of BadgerDB.
 *
 * @section LICENSE
 * Copyright (c) 2012 Database Group, Computer Sciences Department, University of Wisconsin-Madison.
 */

#pragma once

#include <iostream>
#include <string>
#include "string.h"
#include <sstream>

#include "types.h"
#include "page.h"
#include "file.h"
#include "buffer.h"

namespace badgerdb
{

/**
 * @brief Datatype enumeration type.
 */
enum Datatype
{
	INTEGER = 0,
	DOUBLE = 1,
	STRING = 2
};

/**
 * @brief Scan operations enumeration. Passed to BTreeIndex::startScan() method.
 */
enum Operator
{ 
	LT, 	/* Less Than */
	LTE,	/* Less Than or Equal to */
	GTE,	/* Greater Than or Equal to */
	GT		/* Greater Than */
};

/**
 * @brief Size of String key.
 */
const  int STRINGSIZE = 10;

/**
 * @brief Number of key slots in B+Tree leaf for INTEGER key.
 */
//                                          sibling,parent ptr     size               key               rid
const  int INTARRAYLEAFSIZE = ( Page::SIZE - 2*sizeof( PageId )  - sizeof(int )) / ( sizeof( int ) + sizeof( RecordId ) );

/**
 * @brief Number of key slots in B+Tree leaf for DOUBLE key.
 */
//                                              parent,sibling ptr    size                 key               rid
const  int DOUBLEARRAYLEAFSIZE = ( Page::SIZE - 2*sizeof( PageId ) - sizeof(int) ) / ( sizeof( double ) + sizeof( RecordId ) );

/**
 * @brief Number of key slots in B+Tree leaf for STRING key.
 */
//                                             parent,sibling ptr      size                   key                      rid
const  int STRINGARRAYLEAFSIZE = ( Page::SIZE - 2*sizeof( PageId ) - sizeof(int) ) / ( STRINGSIZE * sizeof(char) + sizeof( RecordId ) );

/**
 * @brief Number of key slots in B+Tree non-leaf for INTEGER key.
 */
//                                                     level     parent pageNo        parent            size          key       pageNo
const  int INTARRAYNONLEAFSIZE = ( Page::SIZE - sizeof( int ) /*- sizeof( PageId )*/ - sizeof(PageId) - sizeof(int) ) / ( sizeof( int ) + sizeof( PageId ) );

/**
 * @brief Number of key slots in B+Tree leaf for DOUBLE key.
 */
//                                                        level        parent pageNo     parent            size                   key            pageNo   -1 due to structure padding
const  int DOUBLEARRAYNONLEAFSIZE = (( Page::SIZE - sizeof( int ) /*- sizeof( PageId )*/- sizeof(PageId) - sizeof(int)) / ( sizeof( double ) + sizeof( PageId ) )) - 1;

/**
 * @brief Number of key slots in B+Tree leaf for STRING key.
 */
//                                                        level        parent pageNo    parent            size                key                   pageNo
const  int STRINGARRAYNONLEAFSIZE = ( Page::SIZE - sizeof( int ) /*- sizeof( PageId )*/- sizeof(PageId) - sizeof(int)) / ( STRINGSIZE * sizeof(char) + sizeof( PageId ) );

/**
 * @brief Structure to store a key-rid pair. It is used to pass the pair to 
 * functions that add to or make changes to the leaf node pages of the tree.
 * Is templated for the key member.
 */
template <class T>
class RIDKeyPair{
public:
	RecordId rid;
	T key;
	void set( RecordId r, T k)
	{
		rid = r;
		key = k;
	}
};

/**
 * @brief Structure to store a key page pair which is used to pass the key and 
 * page to functions that make any modifications to the non leaf pages of the
 * tree.
*/
template <class T>
class PageKeyPair{
public:
	PageId pageNo;
	T key;
	void set( int p, T k)
	{
		pageNo = p;
		key = k;
	}
};

/**
 * @brief Overloaded operator to compare the key values of two rid-key pairs
 * and if they are the same compares to see if the first pair has
 * a smaller rid.pageNo value.
*/
template <class T>
bool operator<( const RIDKeyPair<T>& r1, const RIDKeyPair<T>& r2 )
{
	if( r1.key != r2.key )
		return r1.key < r2.key;
	else
		return r1.rid.page_number < r2.rid.page_number;
}

/**
 * @brief The meta page, which holds metadata for Index file, is always first 
 * page of the btree index file and is cast to the following structure to store
 * or retrieve information from it.
 * Contains the relation name for which the index is created, the byte offset
 * of the key value on which the index is made, the type of the key and the page
 * no of the root page. Root page starts as page 2 but since a split can occur
 * at the root the root page may get moved up and get a new page no.
*/
struct IndexMetaInfo{
  /**
   * Name of base relation.
   */
	char relationName[20];

  /**
   * Offset of attribute, over which index is built, inside the record stored
   * in pages.
   */
	int attrByteOffset;

  /**
   * Type of the attribute over which index is built.
   */
	Datatype attrType;

  /**
   * Page number of root page of the B+ Tree inside the file index file.
   */
	PageId rootPageNo;
};

/*
Each node is a page, so once we read the page in we just cast the pointer to the
page to this struct and use it to access the parts
These structures basically are the format in which the information is stored in
the pages for the index file depending on what kind of node they are. The level
memeber of each non leaf structure seen below is set to 1 if the nodes at this
level are just above the leaf nodes. Otherwise set to 0.
*/

/**
 * @brief Structure for all non-leaf nodes when the key is of INTEGER type.
*/
struct NonLeafNodeInt{
  /**
   * Level of the node in the tree.
   */
	int level;
  
  /**
   * Number of entries in this node
   */
    int size;

  /**
   * Stores keys.
   */
	int keyArray[ INTARRAYNONLEAFSIZE ];

  /**
   * Stores page numbers of child pages which themselves are other non-leaf/leaf
   * nodes in the tree.
   */
	PageId pageNoArray[ INTARRAYNONLEAFSIZE + 1 ];
};

/**
 * @brief Structure for all non-leaf nodes when the key is of DOUBLE type.
*/
struct NonLeafNodeDouble{
  /**
   * Level of the node in the tree.
   */
	int level;

  /**
   * Number of entries in this node
   */
    int size;

  /**
   * Stores keys.
   */
	double keyArray[ DOUBLEARRAYNONLEAFSIZE ];

  /**
   * Stores page numbers of child pages which themselves are other non-leaf/leaf
   * nodes in the tree.
   */
	PageId pageNoArray[ DOUBLEARRAYNONLEAFSIZE + 1 ];
};

/**
 * @brief Structure for all non-leaf nodes when the key is of STRING type.
*/
struct NonLeafNodeString{
  /**
   * Level of the node in the tree.
   */
	int level;

  /**
   * Number of entries in this node
   */
    int size;

  /**
   * Stores keys.
   */
	char keyArray[ STRINGARRAYNONLEAFSIZE ][ STRINGSIZE ];

  /**
   * Stores page numbers of child pages which themselves are other non-leaf/leaf
   * nodes in the tree.
   */
	PageId pageNoArray[ STRINGARRAYNONLEAFSIZE + 1 ];
};

/**
 * @brief Structure for all leaf nodes when the key is of INTEGER type.
*/
struct LeafNodeInt{

  /**
   * Number of entries in this node
   */
    int size;


  /**
   * Stores keys.
   */
	int keyArray[ INTARRAYLEAFSIZE ];

  /**
   * Stores RecordIds.
   */
	RecordId ridArray[ INTARRAYLEAFSIZE ];

  /**
   * Page number of the leaf on the right side.
	 * This linking of leaves allows to easily move from one leaf to the next
     * leaf during index scan.
   */
	PageId rightSibPageNo;
};

/**
 * @brief Structure for all leaf nodes when the key is of DOUBLE type.
*/
struct LeafNodeDouble{

  /**
   * Number of entries in this node
   */
    int size;

  /**
   * Stores keys.
   */
	double keyArray[ DOUBLEARRAYLEAFSIZE ];

  /**
   * Stores RecordIds.
   */
	RecordId ridArray[ DOUBLEARRAYLEAFSIZE ];

  /**
   * Page number of the leaf on the right side.
	 * This linking of leaves allows to easily move from one leaf to the next
     * leaf during index scan.
   */
	PageId rightSibPageNo;
};

/**
 * @brief Structure for all leaf nodes when the key is of STRING type.
*/
struct LeafNodeString{

  /**
   * Number of entries in this node
   */
    int size;

  /**
   * Stores keys.
   */
	char keyArray[ STRINGARRAYLEAFSIZE ][ STRINGSIZE ];

  /**
   * Stores RecordIds.
   */
	RecordId ridArray[ STRINGARRAYLEAFSIZE ];

  /**
   * Page number of the leaf on the right side.
	 * This linking of leaves allows to easily move from one leaf to the next
     * leaf during index scan.
   */
	PageId rightSibPageNo;
};

/**
 * @brief BTreeIndex class. It implements a B+ Tree index on a single attribute
 * of a relation. This index supports only one scan at a time.
*/
class BTreeIndex {

 private:

  /**
   * File object for the index file.
   */
	File		*file;

  /**
   * Buffer Manager Instance.
   */
	BufMgr	*bufMgr;

  /**
   * Page number of meta page.
   */
	PageId	headerPageNum;

  /**
   * page number of root page of B+ tree inside index file.
   */
	PageId	rootPageNum;

  /**
   * Datatype of attribute over which index is built.
   */
	Datatype	attributeType;

  /**
   * Offset of attribute, over which index is built, inside records. 
   */
	int 		attrByteOffset;

  /**
   * Number of keys in leaf node, depending upon the type of key.
   */
	int		leafOccupancy;

  /**
   * Number of keys in non-leaf node, depending upon the type of key.
   */
	int		nodeOccupancy;


	// MEMBERS SPECIFIC TO SCANNING

  /**
   * True if an index scan has been started.
   */
	bool		scanExecuting;

  /**
   * Index of next entry to be scanned in current leaf being scanned.
   */
	int		nextEntry;

  /**
   * Page number of current page being scanned.
   */
	PageId	currentPageNum;

  /**
   * Current Page being scanned.
   */
	Page		*currentPageData;

  /**
   * Low INTEGER value for scan.
   */
	int		lowValInt;

  /**
   * Low DOUBLE value for scan.
   */
	double	lowValDouble;

  /**
   * Low STRING value for scan.
   */
	std::string	lowValString;
	char lowStringKey[STRINGSIZE] ;

  /**
   * High INTEGER value for scan.
   */
	int		highValInt;

  /**
   * High DOUBLE value for scan.
   */
	double	highValDouble;

  /**
   * High STRING value for scan.
   */
	std::string highValString;
	char highStringKey[STRINGSIZE];


    /**
     * Low Operator. Can only be GT(>) or GTE(>=).
     */
    Operator	lowOp;

    /**
     * High Operator. Can only be LT(<) or LTE(<=).
     */
    Operator	highOp;


    /**
     * Build the intial BTree from given relation
     * @param relationName Name of the file that stores the relation
     */
    const void buildBTree(const std::string & relationName);


    /**
     * print the whole tree
     */
    template <class T, class T_NonLeafNode, class T_LeafNode>
      const void printTree();


    /**
     * Find Leaf page to insert the record in
     *
     * @param pageNo given leaf/non-leaf node, find the key or trace down further
     * @param key key
     *
     * @return return the leaf node page number that the key should be inserted
     *  into.
     */
    template<class T, class T_NonLeafNode>
      const PageId findLeafNode(PageId pageNo, T & key);


    /**
     * Find the parent node of the given node with the help of key.
     *
     * @param childPageNo the page number that the parent should points to
     * @param key key, usually the last key in the page
     *
     * @return parent page number. If no parent, return 0.
     */
    template<class T, class T_NonLeafNode, class T_LeafNode>
      const PageId findParentOf( PageId childPageNo, T &key);



    /**
     * insert the RIDKeyPair into a leaf node
     *
     * @param pageNo Page number of the leaf node
     * @parem rkpair RIDKeyPair, contains rid and key
     * @param key	 Key to insert, pointer to integer/double/char string
     * @param rid	 Record ID of a record whose entry is getting inserted into the index.
     */
    template<class T, class T_NonLeafNode, class T_LeafNode> 
      const void insertLeafNode(PageId pageNo, RIDKeyPair<T> rkpair);



    /**
     * Split leaf node.
     * If necessary, the split will propagate upward until no split is needed.
     * Worst case the root get splitted
     * @param pageNo the node to be splitted
     *
     * @return PageId of the newly created page
     */
    template<class T, class T_NonLeafNode, class T_LeafNode>
      const PageId splitLeafNode( PageId pageNo);
    // haiyun, try be the same as splitNonLeafNode



    /**
     * insert the RIDKeyPair into a non-leaf node
     *
     * @param pageNo Page number of the node
     * @param key	 Key to insert, pointer to integer/double/char string
     * @param rid	 Record ID of a record whose entry is getting inserted into the index.
     */
    template<class T, class T_NonLeafNode, class T_LeafNode>
      const void insertNonLeafNode(PageId pageNo, T &key, PageId childPageNo);
//       const void insertNonLeafNode(PageId pageNo, PageKeyPair<T> pkpair);



    /**
     * Split non-leaf node.
     * If necessary, the split will propagate upward until no split is needed.
     * Worst case the root get splitted
     *
     *
     * @param pageNo the node to be splitted
     * @param insertLeftNode after the split, whether new key is left or right
     *
     * @return PageId of the newly created page
     */
    template<class T, class T_NonLeafNode, class T_LeafNode>
      const PageId splitNonLeafNode( PageId pageNo, int midIndex);






    /**
     * Delete a key from a leaf node.
     * Note that it is possible that the key is not in this page that is pointed
     * by the pageNo. The pageNo is the PageId that the key is supposed to be in
     * but there is no guarantee that the key exists.
     * 
     * @param pageNo PageId of the leaf to delete key from.
     * @param key    Pointer to the key that to be deleted.
     */
    template<class T, class T_NonLeafNode, class T_LeafNode>
      const void deleteLeafNode(PageId pageNo, T& key);


    /**
     * Merge two succssive leaf nodes
     *
     * @param firstPageNo
     * @param secondPageNo
     */
    template<class T, class T_NonLeafNode, class T_LeafNode>
      const void mergeLeafNode(PageId firstPageNo, PageId secondPageNo);



    /**
     * Delete a key from a nonleaf node.
     * Unlike delete key from a leaf node, the key is guaranteed not to exist
     * in the page, what we deleted from this nonleaf page is the last key that
     * is smaller than the target key.
     * 
     * @param pageNo PageId of the nonleaf to delete key from.
     * @param key    Pointer to the key that to be deleted.
     */
    template<class T, class T_NonLeafNode, class T_LeafNode>
      const void deleteNonLeafNode(PageId pageNo, T& key);


    /**
     * Merge two succssive non leaf nodes
     *
     * @param firstPageNo
     * @param secondPageNo
     */
    template<class T, class T_NonLeafNode, class T_LeafNode>
      const void mergeNonLeafNode(PageId firstPageNo, PageId secondPageNo);



    /**
     * generic start scan, use c++ template
     *
     * @param lowVal low value
     * @param highal high value
     */
    template<class T, class T_NonLeafNode, class T_LeafNode>
      const void startScanHelper(T & lowVal, T & highVal);



    /**
     * generic scan next method, with c++ template
     * @param outRid 
     * @param lowVal
     * @param highVal
     */
    template <class T, class T_LeafNode >
      const void scanNextHelper(RecordId & outRid, T lowVal, T highVal);



    /**
     * More complete nextEntry++. 
     * Do size check. If next entry is valid, simple ++, else shift to next
     * page and set nextEntry to 0.
     *
     * @param thisPage Page pointer of current page
     */
    template<class T_NodeType>
      const void shiftToNextEntry(T_NodeType *thisPage);






 public:

    /**
     * BTreeIndex Constructor. 
     * Check to see if the corresponding index file exists. If so, open the file.
     * If not, create it and insert entries for every tuple in the base relation
     * using FileScan class.
     *
     * @param relationName    Name of file.
     * @param outIndexName    Return the name of index file.
     * @param bufMgrIn		Buffer Manager Instance
     * @param attrByteOffset  Offset of attribute, over which index is to be built, in the record
     * @param attrType		Datatype of attribute over which index is built
     * @throws  BadIndexInfoException     If the index file already exists for the corresponding attribute, but values in metapage(relationName, attribute byte offset, attribute type etc.) do not match with values received through constructor parameters.
     */
    BTreeIndex(const std::string & relationName, std::string & outIndexName,
        BufMgr *bufMgrIn,	const int attrByteOffset,	const Datatype attrType);


    /**
     * BTreeIndex Destructor. 
     * End any initialized scan, flush index file, after unpinning any pinned
     * pages, from the buffer manager
     * and delete file instance thereby closing the index file.
     * Destructor should not throw any exceptions. All exceptions should be
     * caught in here itself. 
     * */
    ~BTreeIndex();


    /**
     * Insert a new entry using the pair <value,rid>. 
     * Start from root to recursively find out the leaf to insert the entry in.
     * The insertion may cause splitting of leaf node.
     * This splitting will require addition of new leaf page number entry into
     * the parent non-leaf, which may in-turn get split.
     * This may continue all the way upto the root causing the root to get 
     * split. If root gets split, metapage needs to be changed accordingly.
     * Make sure to unpin pages as soon as you can.
     * @param key	 Key to insert, pointer to integer/double/char string
     * @param rid	 Record ID of a record whose entry is getting inserted into the index.
     **/
    const void insertEntry(const void* key, const RecordId rid);


    /**
     * Delete a key.
     * Start from root to recursively find out the leaf that contains the key.
     * The deletion may cause key redistribution or leaf nodes merge.
     * The merge may leads to deletion of a key in the parent node.
     * If the last key in the root node is deleted, new root is choosed from
     * its children and the metapage needs to be updated.
     *
     * @param key   Key to delete.
     */
    const void deleteEntry(const void* key);



    /**
     * public method print the whole tree
     */
      const void printTree();



      /**
       * Begin a filtered scan of the index.  For instance, if the method is called 
       * using ("a",GT,"d",LTE) then we should seek all entries with a value 
       * greater than "a" and less than or equal to "d".
       * If another scan is already executing, that needs to be ended here.
       * Set up all the variables for scan. Start from root to find out the leaf 
       * page that contains the first RecordID that satisfies the scan parameters.
       * Keep that page pinned in the buffer pool.
       * @param lowVal	Low value of range, pointer to integer / double / char string
       * @param lowOp		Low operator (GT/GTE)
       * @param highVal	High value of range, pointer to integer / double / char string
       * @param highOp	High operator (LT/LTE)
       * @throws  BadOpcodesException If lowOp and highOp do not contain one of their their expected values 
       * @throws  BadScanrangeException If lowVal > highval
	 * @throws  NoSuchKeyFoundException If there is no key in the B+ tree that satisfies the scan criteria.
	**/
	const void startScan(const void* lowVal, const Operator lowOp, const void* highVal, const Operator highOp);


  /**
	 * Fetch the record id of the next index entry that matches the scan.
	 * Return the next record from current page being scanned. If current page has been scanned to its entirety, move on to the right sibling of current page, if any exists, to start scanning that page. Make sure to unpin any pages that are no longer required.
   * @param outRid	RecordId of next record found that satisfies the scan criteria returned in this
	 * @throws ScanNotInitializedException If no scan has been initialized.
	 * @throws IndexScanCompletedException If no more records, satisfying the scan criteria, are left to be scanned.
	**/
	const void scanNext(RecordId& outRid);  // returned record id


  /**
	 * Terminate the current scan. Unpin any pinned pages. Reset scan specific variables.
	 * @throws ScanNotInitializedException If no scan has been initialized.
	**/
	const void endScan();
	
};

}
