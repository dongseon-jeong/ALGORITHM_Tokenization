### 0. 라이브러리 불러오기
library(tidyverse)
library(data.table)
library(RcppMeCab)
library(KoNLP)
library(foreach)
library(doParallel)
library(furrr)
library(progressr)


rm(list=ls())


### 1.파일 불러오기
file_path = "your_path"
df <-fread(file.path(file_path,"review.csv"),encoding = "UTF-8")

### 2.전처리 
# a.하이라이트 뽑기
df <- df %>% mutate(num1=str_split_fixed(df$keywords,",",2)[,1],
                   num2=str_split_fixed(df$keywords,",",2)[,2],
                   highlight=substr(message,start=as.integer(num1)+1,stop=as.integer(num2)+1)) %>%
             select(brand_code, review_id, review_tag_type_id, highlight)


# b.전처리 하이라이트 리스트 정리/중복제거/na제거
re_list <- unique(df$highlight)
re_list <- re_list[!is.na(re_list)]
re_list <- re_list[re_list != ""]

# c.매칭용 데이터 프레임 만들기
num <- c(1:length(re_list))
col1 <- data.frame(num)
re_df  <- cbind(col1,re_list)

### 3.병렬 연산 
# a.준비
numCores <- detectCores() - 2
myCluster <- makeCluster(numCores)
registerDoParallel(myCluster)

# b.형태소 분리 펑션
f_result <- function(x){
  f_re <- RcppMeCab::posParallel(x,join=FALSE,format="data.frame")
  f_re$num <- which(re_list == x)
  return(f_re)
}

# c.병렬 진행
with_progress({
  p <- progressor(steps = 100)
  f_result_df <- 
  foreach(x = re_list, .combine = rbind) %dopar% {
    # p() 
    f_result(x) 
  }
})

# d.병렬 종료
stopCluster(myCluster)

### 4.전처리
# a.형태소 분리 결과 매칭용 테이블에 합치기
re_df <-
  re_df %>% left_join(f_result_df %>% mutate_at(vars(num),as.integer), by ="num")

# b.불필요한 형태소 제거
re_df <- 
  re_df %>%  mutate(nn=grepl("\n|\r",token),nnn=grepl("[^ \uAC00-\uD7A30a-zA-Z]",token)) %>% 
  filter(nn == FALSE & nnn == FALSE) %>%   select(-nn,-nnn,-subtype) %>% rename(highlight=re_list)

# c.하이라이트 테이블에 형태소 붙이기
df <- 
  as.data.table(re_df)[as.data.table(df), on = .(highlight), allow.cartesian=TRUE] %>% 
  select(doc_id,sentence_id,token_id,token,pos,review_id,review_tag_type_id,brand_code,highlight)
  
# d.명사 동사 형용사 선택
ok_pos <- c("^NN|^VA|^VV|^VX|^XR|MAG" )
not_pos <- c("^NNB")

df <- df[!is.na(df$pos),]

df <- df[grepl(ok_pos, df$pos),]

df <- df[!grepl(  not_pos, df$pos),]




### 5.결과 파일 저장
fwrite(df,file.path(file_path,"tokens.csv"))

