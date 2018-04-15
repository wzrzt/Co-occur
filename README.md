# WareEmbedding
Ware to vector imitating word to vector.  
  
  
class OrderManager: query new orders   
class WareIndex: add index to each ware, and the index of each ware is the position of the ware in cooccurance matrix.   
class CooCcurMatix: Calculate cooccurance matrix for all wares occurs in orders.   
function PPMI: convert cooccurance matrix to PPMI matrix   
Each row or column of the PPMI matrix is a vector that presents a ware. For further calculation, it needs reduction. We use SVD here.   
TSNE can show the vector of wares in 3 or 2 dimensions.   

