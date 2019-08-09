<?php

function strip_content_tags($content)
{
    return trim(strtr(strip_tags($content, '<p><br>'),
        array(
            '<br />' => '<br/>',
            '</p>' => '',
            '&nbsp;' => ' ',
            '&middot;' => '˙',
            '\r' => ''
        ) ));

}

/**
 * 等比压缩图片
 * @param $im
 * @param $maxWidth
 * @param $maxHeight
 * @param $name
 * @param $type
 */
function ResizeImage($im,$maxWidth,$maxHeight,$name,$type){
    //取得当前图片大小
    $width  = imagesx($im);
    $height = imagesy($im);
    //生成缩略图的大小
    if(($maxWidth && $width > $maxWidth) || ($maxHeight && $height > $maxHeight))
    {

        $widthRatio         = $heightRatio      = $ratio      = 0;
        $resizeWidth_tag    = $resizeHeight_tag = false;

        if($maxWidth && $width>$maxWidth)
        {
            $widthRatio = $maxWidth/$width;
            $resizeWidth_tag = true;
        }
        if($maxHeight && $height>$maxHeight)
        {
            $heightRatio = $maxHeight/$height;
            $resizeHeight_tag = true;
        }
        if($resizeWidth_tag && $resizeHeight_tag)
        {
            if($widthRatio<$heightRatio)
                $ratio = $widthRatio;
            else
                $ratio = $heightRatio;
        }
        if($resizeWidth_tag && !$resizeHeight_tag)
        {
            $ratio = $widthRatio;
        }
        if($resizeHeight_tag && !$resizeWidth_tag)
        {
            $ratio = $heightRatio;
        }
        $newWidth = $width * $ratio;
        $newHeight = $height * $ratio;
        if(function_exists("imagecopyresampled")){
            $newImage = imagecreatetruecolor($newWidth, $newHeight);
            imagecopyresampled($newImage, $im, 0, 0, 0, 0, $newWidth, $newHeight, $width, $height);
        }else{
            $newImage = imagecreate($newWidth, $newHeight);
            imagecopyresized($newImage, $im, 0, 0, 0, 0, $newWidth, $newHeight, $width, $height);
        }
        ImageJpeg ($newImage,'static/images/'.$name . '.'.$type);
        ImageDestroy ($newImage);
    }else{
        ImageJpeg ($im,'static/images/'.$name . '.'.$type);
    }
}


function normal_sort($a,$b) : int
{
    if ($a == $b) return 0;

    if($a < $b) return -1;
    if ($a > $b) return 1;
}

function space_sort($a,$b) : int
{
    return $a<=>$b;
}
